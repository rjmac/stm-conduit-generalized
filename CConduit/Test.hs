{-# OPTIONS_GHC -fno-full-laziness #-}
{-# LANGUAGE BangPatterns #-}

module CConduit.Test where

import Control.Concurrent.MVar (MVar, newMVar, withMVar)
import System.IO.Unsafe (unsafePerformIO)
import Control.Concurrent (threadDelay)
import Control.Monad.IO.Class
import Conduit
import CConduit
import Criterion.Main

m :: MVar ()
m = unsafePerformIO $ newMVar ()
{-# NOINLINE m #-}

lPutStrLn :: (MonadIO m) => String -> m ()
lPutStrLn s = liftIO $ withMVar m $ \_ -> putStrLn s

sleepyProducer :: (MonadIO m) => Int -> Source m Int
sleepyProducer n = loop 0
  where loop i = do
          lPutStrLn $ "Producing " ++ show i
          yield i
          liftIO $ do
            threadDelay n
            lPutStrLn "Producer awake"
          loop (i + 1)

sleepyConsumer :: (MonadIO m) => Int -> Sink Int m ()
sleepyConsumer n = awaitForever $ \i -> liftIO $ do
  lPutStrLn $ "Consuming " ++ show i
  threadDelay n
  lPutStrLn "Consumer awake"

sleepyTransformer :: (MonadIO m) => Int -> Conduit Int m Int
sleepyTransformer n = awaitForever $ \i -> do
  let o = i*2
  liftIO $ do
    lPutStrLn $ "Received " ++ show i
    threadDelay n
    lPutStrLn $ "Yielding " ++ show o
  yield o

fastProducer :: (Monad m) => Source m Int
fastProducer = loop 0
  where loop !i = do
          yield i
          loop (i + 1)

fastTransformer :: (Monad m) => Conduit Int m Int
fastTransformer = awaitForever $ \x -> yield $! 2*x

fastConsumer :: Int -> Sink Int IO Int
fastConsumer n = loop 0
  where loop !i = do
          Just x <- await
          if i >= n then return x else loop (i+1)

go :: IO ()
go = do
  -- sleepyProducer 100000 $$& fastTransformer $=& sleepyConsumer 1100000
  -- let p = bufferToFile 10 (Just 100) "/tmp" (fastProducer $=& fastTransformer) (sleepyConsumer 100000)
  -- runResourceT $ runCConduit p
  defaultMain [ bench "it" $ whnfIO $ runCConduit (fastProducer $=& fastTransformer $=& fastTransformer $=& fastTransformer $=& fastConsumer 100000)
              ]
