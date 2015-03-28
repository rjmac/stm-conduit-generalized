{-# LANGUAGE GADTs, RankNTypes, FlexibleContexts, ScopedTypeVariables, RecordWildCards, LambdaCase, TypeFamilies, MultiParamTypeClasses, ConstraintKinds, CPP #-}

module CConduit (CConduit, CFConduit, ($=&), ($$&), buffer, bufferToFile, runCConduit) where

import Control.Monad.Trans.Resource
import Control.Exception (finally)
import Control.Applicative
import Control.Monad hiding (forM_)
import Data.Foldable (forM_)
import Control.Concurrent.STM
import qualified Control.Concurrent.Async as A
import Control.Concurrent.Async.Lifted hiding (link2)
import Control.Monad.IO.Class
import Data.Void
import Data.Serialize
import Conduit
import System.IO
import System.Directory (removeFile)
import qualified Data.Conduit.Binary as CB
import qualified Data.Conduit.Cereal as C
import qualified Data.Conduit.List as CL
import GHC.Prim

#ifdef USE_UNAGI_CHAN
import Control.Concurrent.Chan.Unagi.Bounded
#endif

-- In order to allow the user to freely mix and match ordinary
-- conduits and concurrent conduits (which may or may not involve
-- hitting the disk!) we'll do lots of type magic

-- | Like '$=', but the two conduits will execute concurrently when
-- run.  This is 'buffer' with a default buffer size of 64.
($=&) :: (CCatable c1 c2) => c1 i x m () -> c2 x o m r -> LeastCConduit c1 c2 i o m r
a $=& b = buffer 64 a b

-- | Like '$$', but the two conduits will run concurrently.  This is
-- '($=&)' combined with 'runCConduit'.
($$&) :: (CCatable c1 c2, CRunnable (LeastCConduit c1 c2), RunConstraints (LeastCConduit c1 c2) m) => c1 () x m () -> c2 x Void m r -> m r
a $$& b = runCConduit (a $=& b)

-- | Determines the result type of concurent conduit when two conduits
-- are combined with '$=&'.
type family LeastCConduit a b where
  LeastCConduit ConduitM ConduitM = CConduit
  LeastCConduit ConduitM CConduit = CConduit
  LeastCConduit ConduitM CFConduit = CFConduit

  LeastCConduit CConduit ConduitM = CConduit
  LeastCConduit CConduit CConduit = CConduit
  LeastCConduit CConduit CFConduit = CFConduit

  LeastCConduit CFConduit ConduitM = CFConduit
  LeastCConduit CFConduit CConduit = CFConduit
  LeastCConduit CFConduit CFConduit = CFConduit

-- | Conduits are concatenable; this class describes how.
class CCatable c1 c2 where
  -- | Like '$=', but the two conduits will execute concurrently when
  -- run, with upto the specified number of items bufferd.
  buffer :: Int -> c1 i x m () -> c2 x o m r -> LeastCConduit c1 c2 i o m r

instance CCatable ConduitM ConduitM where
  buffer i a b = buffer i (Single a) (Single b)

instance CCatable ConduitM CConduit where
  buffer i a b = buffer i (Single a) b

instance CCatable ConduitM CFConduit where
  buffer i a b = buffer i (FSingle a) b

instance CCatable CConduit ConduitM where
  buffer i a b = buffer i a (Single b)

instance CCatable CConduit CConduit where
  buffer i (Single a) b = Multiple i a b
  buffer i (Multiple i' a as) b = Multiple i' a (buffer i as b)

instance CCatable CConduit CFConduit where
  buffer i (Single a) b = buffer i (FSingle a) b
  buffer i (Multiple i' a as) b = buffer i (FMultiple i' a $ asCFConduit as) b

instance CCatable CFConduit ConduitM where
  buffer i a b = buffer i a (FSingle b)

instance CCatable CFConduit CConduit where
  buffer i a (Single b) = buffer i a (FSingle b)
  buffer i a (Multiple i' b bs) = buffer i a (FMultiple i' b $ asCFConduit bs)

instance CCatable CFConduit CFConduit where
  buffer i (FSingle a) b = FMultiple i a b
  buffer i (FMultiple i' a as) b = FMultiple i' a (buffer i as b)
  buffer i (FMultipleF bufsz dsksz tmpDir a as) b = FMultipleF bufsz dsksz tmpDir a (buffer i as b)

-- | Conduits are, once there's a producer on one end and a consumer
-- on the other, runnable.
class CRunnable c where
  type RunConstraints c (m :: * -> *) :: Constraint
  -- | Execute a conduit concurrently.  The equivalent of 'runConduit'.
  runCConduit :: (RunConstraints c m) => c () Void m r -> m r

instance CRunnable ConduitM where
  type RunConstraints ConduitM m = (Monad m)
  runCConduit = runConduit

instance CRunnable CConduit where
  type RunConstraints CConduit m = (MonadBaseControl IO m, MonadIO m)
  runCConduit (Single c) = runConduit c
  runCConduit (Multiple bufsz c cs) = do
    (i, o) <- liftIO $ newQueue bufsz
    withAsync (sender i c) $ \c' ->
      stage o c' cs

instance CRunnable CFConduit where
  type RunConstraints CFConduit m = (MonadBaseControl IO m, MonadIO m, MonadResource m)
  runCConduit (FSingle c) = runConduit c
  runCConduit (FMultiple bufsz c cs) = do
    (i, o) <- liftIO $ newQueue bufsz
    withAsync (sender i c) $ \c' ->
      fstage (receiver o) c' cs
  runCConduit (FMultipleF bufsz filemax tempDir c cs) = do
    context <- liftIO $ BufferContext <$> newTBQueueIO bufsz
                                      <*> newTQueueIO
                                      <*> newTVarIO filemax
                                      <*> newTVarIO False
                                      <*> pure tempDir
    withAsync (fsender context c) $ \c' ->
      fstage (freceiver context) c' cs

-- | A "concurrent conduit", in which the stages run in parallel with
-- a buffering queue between them.
data CConduit i o m r where
  Single :: ConduitM i o m r -> CConduit i o m r
  Multiple :: Int -> ConduitM i x m () -> CConduit x o m r -> CConduit i o m r

infixr 2 $=&
infixr 0 $$&

-- C.C.A.L's link2 has the wrong type:  https://github.com/maoe/lifted-async/issues/16
link2 :: MonadBase IO m => Async a -> Async b -> m ()
link2 = (liftBase .) . A.link2

sender :: (MonadIO m) => I (Maybe o) -> ConduitM () o m () -> m ()
sender chan input = do
  input $$ mapM_C (send chan . Just)
  send chan Nothing

stage :: (MonadBaseControl IO m, MonadIO m) => O (Maybe i) -> Async x -> CConduit i Void m r -> m r
stage chan prevAsync (Single c) =
  withAsync (receiver chan $$ c) $ \c' -> do
    link2 prevAsync c'
    wait c'
stage chan prevAsync (Multiple bufsz c cs) = do
  (i, o) <- liftIO $ newQueue bufsz
  withAsync (sender i $ receiver chan =$= c) $ \c' -> do
    link2 prevAsync c'
    stage o c' cs

receiver :: (MonadIO m) => O (Maybe o) -> ConduitM () o m ()
receiver chan = do
  mx <- recv chan
  case mx of
   Nothing -> return ()
   Just x -> yield x >> receiver chan

-- | A "concurrent conduit", in which the stages run in parallel with
-- a buffering queue and possibly a disk file between them.
data CFConduit i o m r where
  FSingle :: ConduitM i o m r -> CFConduit i o m r
  FMultiple :: Int -> ConduitM i x m () -> CFConduit x o m r -> CFConduit i o m r
  FMultipleF :: (Serialize x) => Int -> Maybe Int -> String -> ConduitM i x m () -> CFConduit x o m r -> CFConduit i o m r

class CFConduitLike a where
  asCFConduit :: a i o m r -> CFConduit i o m r

instance CFConduitLike ConduitM where
  asCFConduit = FSingle

instance CFConduitLike CConduit where
  asCFConduit (Single c) = FSingle c
  asCFConduit (Multiple i c cs) = FMultiple i c (asCFConduit cs)

instance CFConduitLike CFConduit where
  asCFConduit = id

bufferToFile :: (CFConduitLike c1, CFConduitLike c2, Serialize x) => Int -> Maybe Int -> FilePath -> c1 i x m () -> c2 x o m r -> CFConduit i o m r
bufferToFile bufsz dsksz tmpDir c1 c2 = combine (asCFConduit c1) (asCFConduit c2)
  where combine (FSingle a) b = FMultipleF bufsz dsksz tmpDir a b
        combine (FMultiple i a as) b = FMultiple i a (bufferToFile bufsz dsksz tmpDir as b)
        combine (FMultipleF bufsz' dsksz' tmpDir' a as) b = FMultipleF bufsz' dsksz' tmpDir' a (bufferToFile bufsz dsksz tmpDir as b)

data BufferContext m a = BufferContext { chan :: TBQueue a
                                       , restore :: TQueue (Source m a)
                                       , slotsFree :: TVar (Maybe Int)
                                       , done :: TVar Bool
                                       , tempDir :: FilePath
                                       }

fsender :: (MonadIO m, MonadResource m, Serialize x) => BufferContext m x -> ConduitM () x m () -> m ()
fsender bc@BufferContext{..} input = do
  input $$ mapM_C $ \x -> join $ liftIO $ atomically $ do
    (writeTBQueue chan x >> return (return ())) `orElse` do
      action <- persistChan bc
      writeTBQueue chan x
      return action
  liftIO $ atomically $ writeTVar done True

-- Connect a stage to another stage via either an in-memory queue or a disk buffer
fstage :: (MonadBaseControl IO m, MonadIO m, MonadResource m) => ConduitM () i m () -> Async x -> CFConduit i Void m r -> m r
fstage prevStage prevAsync (FSingle c) =
  withAsync (prevStage $$ c) $ \c' -> do
    link2 prevAsync c'
    wait c'
fstage prevStage prevAsync (FMultiple bufsz c cs) = do
  (i, o) <- liftIO $ newQueue bufsz
  withAsync (sender i $ prevStage =$= c) $ \c' -> do
    link2 prevAsync c'
    fstage (receiver o) c' cs
fstage prevStage prevAsync (FMultipleF bufsz dsksz tempDir c cs) = do
  bc <- liftIO $ BufferContext <$> newTBQueueIO bufsz
                               <*> newTQueueIO
                               <*> newTVarIO dsksz
                               <*> newTVarIO False
                               <*> pure tempDir
  withAsync (fsender bc $ prevStage =$= c) $ \c' -> do
    link2 prevAsync c'
    fstage (freceiver bc) c' cs

freceiver :: (MonadIO m) => BufferContext m o -> ConduitM () o m ()
freceiver BufferContext{..} = loop where
  loop = do
    (src, exit) <- liftIO $ atomically $ do
      (readTQueue restore >>= (\action -> return (action, False))) `orElse` do
        xs <- exhaust chan
        isDone <- readTVar done
        return (CL.sourceList xs, isDone)
    src
    unless exit loop

persistChan :: (MonadIO m, MonadResource m, Serialize o) => BufferContext m o -> STM (m ())
persistChan BufferContext{..} = do
  xs <- exhaust chan
  mslots <- readTVar slotsFree
  let len = length xs
  forM_ mslots $ \slots -> check (len < slots)
  filePath <- newEmptyTMVar
  writeTQueue restore $ do
    (path, key) <- liftIO $ atomically $ takeTMVar filePath
    CB.sourceFile path $= do
      C.conduitGet get
      liftIO $ atomically $ modifyTVar slotsFree (fmap (+ len))
      release key
  case xs of
   [] -> return (return ())
   _ -> do
     modifyTVar slotsFree (fmap (subtract len))
     return $ do
       (key, (path, h)) <- allocate (openBinaryTempFile tempDir "conduit.bin") (\(path, h) -> hClose h `finally` removeFile path)
       liftIO $ do
         CL.sourceList xs $= C.conduitPut put $$ CB.sinkHandle h
         hClose h
         atomically $ putTMVar filePath (path, key)

exhaust :: TBQueue a -> STM [a]
exhaust chan = do
  tryReadTBQueue chan >>= \case
    Just x -> do
      xs <- exhaust chan
      return $ x : xs
    Nothing ->
      return []

#ifdef USE_UNAGI_CHAN

type I a = InChan a
type O a = OutChan a

newQueue :: Int -> IO (I a, O a)
newQueue = newChan

recv :: (MonadIO m) => O a -> m a
recv = liftIO . readChan

send :: (MonadIO m) => I a -> a -> m ()
send c = liftIO . writeChan c

#else

newtype I a = I (TBQueue a)
newtype O a = O (TBQueue a)

newQueue :: Int -> IO (I a, O a)
newQueue n = do
  x <- newTBQueueIO n
  return (I x, O x)

recv :: (MonadIO m) => O a -> m a
recv (O c) = liftIO . atomically $ readTBQueue c

send :: (MonadIO m) => I a -> a -> m ()
send (I q) = liftIO . atomically . writeTBQueue q

#endif
