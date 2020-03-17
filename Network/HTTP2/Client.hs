{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

-- | HTTP\/2 client library.
--
--  Example:
--
-- > module Main where
-- >
-- > import Control.Concurrent (forkIO)
-- > import Data.ByteString.Char8 (pack)
-- > import Network.Run.TCP (runTCPClient) -- network-run
-- > import qualified Network.Socket.ByteString as NSB
-- >
-- > import Network.HTTP2.Client
-- >
-- > authority :: String
-- > authority = "127.0.0.1"
-- >
-- > main :: IO ()
-- > main = runTCPClient authority "80" $ \sock -> do
-- >     let conf = Config (NSB.sendAll sock) (NSB.recv sock)
-- >     run conf $ \client -> do
-- >         _ <- forkIO $ client defaultRequest{ requestAuthority = pack authority } print
-- >         client defaultRequest{ requestAuthority = pack authority } print

module Network.HTTP2.Client (
  -- * Connection
    run
  , Config(..)
  , Send
  , Recv
  , Client
  -- * Stream
  , Request
  , Response
  , requestNoBody
  ) where

import Control.Concurrent
import Control.Concurrent.STM
import qualified Control.Exception as E
import Control.Monad (void, forever)
import Data.ByteString (ByteString)
import qualified Data.CaseInsensitive as CI
import Data.IORef (IORef)
import qualified Data.IORef as IORef
import Data.IntMap (IntMap)
import qualified Data.IntMap as I
import Network.HPACK (HeaderList, HeaderTable)
import qualified Network.HPACK as HPACK
import Network.HTTP.Types

import Network.HTTP2.Frame
import Network.HTTP2.Types hiding (Config)

----------------------------------------------------------------

type Send = ByteString -> IO ()
type Recv = Int -> IO ByteString

data Config = Config {
    confSend :: Send
  , confRecv :: Recv
  }

----------------------------------------------------------------

-- | HTTP\/2 request for clients.
type Request = OutObj

----------------------------------------------------------------

-- | HTTP\/2 response for clients.
type Response = InpObj

----------------------------------------------------------------

-- | HTTP\/2 connection.
data Connection = Connection {
    streamNumber   :: TVar Int
  , requestQ       :: RequestQ
  , responseQTable :: ResponseQTable
  }

type RequestQ  = TQueue Req
type ResponseQ = TQueue Rsp
type ResponseQTable = TVar (IntMap ResponseQ)

data Req = ReqHeader StreamId Request
         | ReqBody   StreamId ByteString (IO ByteString)
data Rsp = RspHeader  EndOfStream HeaderTable
         | RspBody    EndOfStream ByteString
--       | RspTrailer EndOfStream HeaderTable
type EndOfStream = Bool

type SendFrame = (FrameFlags -> FrameFlags) -> Int -> FramePayload -> IO ()
type RecvFrame = IO Frame

type EncodeHeader = HeaderList -> IO ByteString
type DecodeHeader = ByteString -> IO HeaderTable

----------------------------------------------------------------

newStream :: Connection -> Request -> IO (StreamId, ResponseQ)
newStream Connection{..} req = atomically $ do
    n <- readTVar streamNumber
    let n' = n + 2
    writeTVar streamNumber n'
    rspQ <- newTQueue
    modifyTVar' responseQTable $ \q -> I.insert n rspQ q
    writeTQueue requestQ $ ReqHeader n req
    return (n, rspQ)

deleteStream :: Connection -> StreamId -> IO ()
deleteStream Connection{..} n = atomically $
    modifyTVar' responseQTable $ \q -> I.delete n q

type Client = Request -> (Response -> IO ()) -> IO ()

-- | Sending an HTTP\/2 request and passing its response
--   to an action.
runClient :: Connection -> Client
runClient conn req f = do
    (sid, q) <- newStream conn req
    response <- recvResponse q
    ret <- f response
    deleteStream conn sid
    return ret
  where
    recvResponse q = do
        RspHeader end ht@(_,_vt) <- atomically $ readTQueue q
        trailerRef <- IORef.newIORef Nothing
        if end then
            return $ InpObj ht Nothing (return "") trailerRef
          else do
            endRef <- IORef.newIORef False
            return $ InpObj ht Nothing (recvResponseBody q endRef trailerRef) trailerRef

recvResponseBody :: ResponseQ -> IORef Bool -> IORef (Maybe HeaderTable)
                 -> IO ByteString
recvResponseBody q endRef trailerRef = do
    finished <- IORef.readIORef endRef
    if finished then
        return ""
      else do
        rsp <- atomically $ readTQueue q
        case rsp of
          RspBody end dat
            | end -> do
                  IORef.writeIORef endRef True
                  return dat
            | otherwise -> do
                  if dat == "" then
                       recvResponseBody q endRef trailerRef
                     else
                       return dat
          RspHeader _end ht -> do -- fixme: not suport continuation
              IORef.writeIORef endRef True
              IORef.writeIORef trailerRef $ Just ht
              return ""

----------------------------------------------------------------

-- | Running HTTP/2 client.
run :: Config -> (Client -> IO ()) -> IO ()
run conf body = E.bracket (openHTTP2Connection conf)
                          teardown
                          (\(conn,_,_) -> body $ runClient conn)
  where
    teardown (_,tid1,tid2) = do
        killThread tid1
        killThread tid2

openHTTP2Connection :: Config -> IO (Connection, ThreadId, ThreadId)
openHTTP2Connection conf@Config{..} = do
    exchangeSettings conf
    (enc,dec) <- newDynamicTables
    conn@Connection{..} <- newConnection
    tid1 <- forkIO $ sender   enc (sendFrame confSend) requestQ
    tid2 <- forkIO $ receiver dec (recvFrame confRecv) responseQTable
    return (conn,tid1,tid2)

exchangeSettings :: Config -> IO ()
exchangeSettings Config{..} = do
    confSend connectionPreface
    sendFrame confSend id 0 initialSettingFrame
    void $ recvFrame confRecv
    void $ recvFrame confRecv
    sendFrame confSend setAck 0 ackSettingsFrame

newDynamicTables :: IO (EncodeHeader, DecodeHeader)
newDynamicTables = do
    etbl <- HPACK.newDynamicTableForEncoding HPACK.defaultDynamicTableSize
    dtbl <- HPACK.newDynamicTableForDecoding HPACK.defaultDynamicTableSize 4096
    let enc = HPACK.encodeHeader HPACK.defaultEncodeStrategy 4096 etbl
        dec = HPACK.decodeTokenHeader dtbl
    return (enc, dec)

newConnection :: IO Connection
newConnection = Connection <$> newTVarIO 1
                           <*> newTQueueIO
                           <*> newTVarIO I.empty

sendFrame :: Send -> (FrameFlags -> FrameFlags) -> Int -> FramePayload -> IO ()
sendFrame send func sid payload = do
    let einfo = encodeInfo func sid
        frame = encodeFrame einfo payload
    send frame

recvFrame :: Recv -> IO Frame
recvFrame recv = do
    (frameId, header) <- decodeFrameHeader <$> recv frameHeaderLength
    let len = payloadLength header
    body <- if len == 0 then return "" else recv len
    let Right payload = decodeFramePayload frameId header body
    return $ Frame header payload

----------------------------------------------------------------

sender :: EncodeHeader -> SendFrame -> RequestQ -> IO ()
sender enc send requestQ = forever $ do
    req <- atomically $ readTQueue requestQ
    case req of
      ReqHeader sid (OutObj hdr _body _) -> do -- fixme
          let hdr' = map (\(k,v) -> (CI.foldedCase k,v)) hdr
          hdrblk <- enc hdr'
          send (setEndHeader.setEndStream) sid $ HeadersFrame Nothing hdrblk
      ReqBody sid bs0 body -> do
          bs1 <- body
          if bs1 == "" then
              send setEndStream sid $ DataFrame bs0
            else do
              send id sid $ DataFrame bs0
              atomically $ writeTQueue requestQ $ ReqBody sid bs1 body

----------------------------------------------------------------

receiver :: DecodeHeader -> RecvFrame -> ResponseQTable -> IO ()
receiver dec recv qtbl = forever $ do
    Frame{..} <- recv
    let FrameHeader{..} = frameHeader
    tbl <- atomically $ readTVar qtbl
    case I.lookup streamId tbl of
      Nothing -> return ()
      Just responseQ -> case framePayload of
          DataFrame bs -> do
              let endStream = testEndStream flags
              atomically $ writeTQueue responseQ $ RspBody endStream bs
          HeadersFrame _ hdrblk -> do
              header <- dec hdrblk
              let endStream = testEndStream flags
              atomically $ writeTQueue responseQ $ RspHeader endStream header
          PriorityFrame _ -> return ()
          RSTStreamFrame _ -> return ()
          SettingsFrame _ -> return ()
          PushPromiseFrame _ _ -> return ()
          PingFrame _ -> return ()
          GoAwayFrame _ _ _ -> return ()
          WindowUpdateFrame _ -> return ()
          ContinuationFrame _ -> return ()
          UnknownFrame _ _ -> return ()

----------------------------------------------------------------

initialSettingFrame :: FramePayload
initialSettingFrame = SettingsFrame [
    (SettingsMaxConcurrentStreams,recommendedConcurrency)
  ]

ackSettingsFrame :: FramePayload
ackSettingsFrame = SettingsFrame []

----------------------------------------------------------------

requestNoBody :: Method -> ByteString -> RequestHeaders -> Request
requestNoBody m p hdr = OutObj hdr' OutBodyNone defaultTrailersMaker
  where
    hdr' = (":method", m)
        : (":authority", "127.0.0.1") -- fixme
        : (":path", p)
        : (":scheme", "http") -- fixme
        : hdr
