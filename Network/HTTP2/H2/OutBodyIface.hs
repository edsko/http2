{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE RankNTypes #-}

module Network.HTTP2.H2.OutBodyIface (
    withOutBodyIface,
) where

import Control.Concurrent.STM
import Control.Exception
import Network.HTTP.Semantics
import Network.HTTP.Semantics.IO
import Network.HTTP2.H2.Context
import Network.HTTP2.H2.Types

----------------------------------------------------------------

data StreamTerminated
    = StreamPushedFinal
    | StreamCancelled
    | StreamOutOfScope
    deriving (Show)
    deriving anyclass (Exception)

----------------------------------------------------------------

withOutBodyIface
    :: Context
    -> Stream
    -> TBQueue StreamingChunk
    -> (forall a. IO a -> IO a)
    -> (OutBodyIface -> IO r)
    -> IO r
withOutBodyIface _ctx _strm tbq unmask k = do
    terminated <- newTVarIO Nothing
    let checkNotTerminated :: STM ()
        checkNotTerminated = do
            mTerminated <- readTVar terminated
            maybe (return ()) throwSTM mTerminated

        iface :: OutBodyIface
        iface =
            OutBodyIface
                { outBodyUnmask = unmask
                , outBodyPush = \b -> atomically $ do
                    checkNotTerminated
                    writeTBQueue tbq $ StreamingBuilder b NotEndOfStream
                , outBodyPushFinal = \b -> atomically $ do
                    checkNotTerminated
                    writeTVar terminated (Just StreamPushedFinal)
                    writeTBQueue tbq $ StreamingBuilder b (EndOfStream Nothing)
                    writeTBQueue tbq $ StreamingFinished Nothing
                , outBodyFlush = atomically $ do
                    checkNotTerminated
                    writeTBQueue tbq StreamingFlush
                , outBodyCancel = \mErr -> atomically $ do
                    mTerminated <- readTVar terminated
                    case mTerminated of
                        Nothing -> do
                            writeTVar terminated (Just StreamCancelled)
                            writeTBQueue tbq $ StreamingCancelled mErr
                        Just _ ->
                            -- Already terminated
                            return ()
                }

        finished :: IO ()
        finished = atomically $ do
            mTerminated <- readTVar terminated
            case mTerminated of
                Nothing -> do
                    writeTVar terminated (Just StreamOutOfScope)
                    writeTBQueue tbq $ StreamingFinished Nothing
                Just _ ->
                    -- Already terminated
                    return ()

    k iface `finally` finished
