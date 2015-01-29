{-# LANGUAGE TupleSections, BangPatterns, RecordWildCards #-}

module Network.HTTP2.Decode (
    decodeFrame
  , decodeFrameHeader
  , parseFrame
  , parseFrameHeader
  , parseFramePayload
  ) where

import Control.Applicative ((<$>), (<*>))
import Control.Monad (void, when)
import Data.Array (Array, listArray, (!))
import qualified Data.Attoparsec.Binary as BI
import qualified Data.Attoparsec.ByteString as B
import Data.Bits (shiftL, (.|.))
import Data.ByteString (ByteString)
import Data.List (isPrefixOf)

import Network.HTTP2.Types

----------------------------------------------------------------
-- atto-parsec can return only String as an error type, sigh.

protocolError :: String
protocolError = show ProtocolError

frameSizeError :: String
frameSizeError = show FrameSizeError

----------------------------------------------------------------

-- | Parsing byte stream to make an HTTP/2 frame.
decodeFrame :: Settings    -- ^ HTTP/2 settings
            -> ByteString  -- ^ Input byte-stream
            -> Either ErrorCodeId (Frame,ByteString) -- ^ (Decoded frame, leftover byte-stream)
decodeFrame settings bs = case B.parse (parseFrame settings) bs of
    B.Done left frame -> Right (frame, left)
    B.Fail _ _ estr   -> Left $ toErrorCode estr
    B.Partial _       -> error "partial"

decodeFrameHeader :: Settings -> ByteString
                  -> Either ErrorCodeId (FrameType, FrameHeader)
decodeFrameHeader settings bs = case B.parseOnly (parseFrameHeader settings) bs of
    Right fh   -> Right fh
    Left  estr -> Left $ toErrorCode estr

toErrorCode :: String -> ErrorCodeId
toErrorCode estr
  | estr' == protocolError  = ProtocolError
  | estr' == frameSizeError = FrameSizeError
  | otherwise               = UnknownError estr' -- fixme
  where
    estr'
      -- attoparsec specific, sigh.
      | "Failed reading: " `isPrefixOf` estr = drop 16 estr
      | otherwise                            = estr

----------------------------------------------------------------

parseFrame :: Settings -> B.Parser Frame
parseFrame settings = do
    (ftyp, header) <- parseFrameHeader settings
    Frame header <$> parseFramePayload ftyp header

----------------------------------------------------------------

parseFrameHeader :: Settings -> B.Parser (FrameType, FrameHeader)
parseFrameHeader settings = do
    i0 <- intFromWord16be
    i1 <- intFromWord8
    let len = (i0 `shiftL` 8) .|. i1
    when (doesExceed settings len) $ fail frameSizeError
    ftyp <- B.anyWord8
    flg <- B.anyWord8
    sid <- streamIdentifier
    case toFrameTypeId ftyp of
        Nothing  -> return ()
        Just typ -> when (isProtocolError settings typ sid) $ fail protocolError
    return $ (ftyp, FrameHeader len flg sid)

doesExceed :: Settings -> PayloadLength -> Bool
doesExceed settings len = len > maxLength
  where
    maxLength = maxFrameSize settings

zeroFrameTypes :: [FrameTypeId]
zeroFrameTypes = [
    FrameSettings
  , FramePing
  , FrameGoAway
  ]

nonZeroFrameTypes :: [FrameTypeId]
nonZeroFrameTypes = [
    FrameData
  , FrameHeaders
  , FramePriority
  , FrameRSTStream
  , FramePushPromise
  , FrameContinuation
  ]

isProtocolError :: Settings -> FrameTypeId -> StreamIdentifier -> Bool
isProtocolError settings typ sid
  | typ `elem` nonZeroFrameTypes && sid == streamIdentifierForSeetings = True
  | typ `elem` zeroFrameTypes && sid /= streamIdentifierForSeetings = True
  | typ == FramePushPromise && not pushEnabled = True
  | otherwise = False
  where
    pushEnabled = establishPush settings

----------------------------------------------------------------

type FramePayloadParser = FrameHeader -> B.Parser FramePayload

payloadParsers :: Array FrameTypeId FramePayloadParser
payloadParsers = listArray (minBound :: FrameTypeId, maxBound :: FrameTypeId)
    [ parseDataFrame
    , parseHeadersFrame
    , parsePriorityFrame
    , parseRstStreamFrame
    , parseSettingsFrame
    , parsePushPromiseFrame
    , parsePingFrame
    , parseGoAwayFrame
    , parseWindowUpdateFrame
    , parseContinuationFrame
    ]

parseFramePayload :: FrameType -> FramePayloadParser
parseFramePayload ftyp header = parsePayload mfid header
  where
    mfid = toFrameTypeId ftyp
    parsePayload Nothing    = parseUnknownFrame ftyp
    parsePayload (Just fid) = payloadParsers ! fid

----------------------------------------------------------------

parseDataFrame :: FramePayloadParser
parseDataFrame header = parseWithPadding header $ \len ->
    DataFrame <$> B.take len

parseHeadersFrame :: FramePayloadParser
parseHeadersFrame header = parseWithPadding header $ \len ->
    if hasPriority then do
        p <- priority
        HeadersFrame (Just p) <$> B.take (len - 5)
    else
        HeadersFrame Nothing <$> B.take len
  where
    hasPriority = testPriority $ flags header

parsePriorityFrame :: FramePayloadParser
parsePriorityFrame _ = PriorityFrame <$> priority

parseRstStreamFrame :: FramePayloadParser
parseRstStreamFrame _ = RSTStreamFrame . toErrorCodeId <$> BI.anyWord32be

parseSettingsFrame :: FramePayloadParser
parseSettingsFrame FrameHeader{..}
  | isNotValid = fail frameSizeError
  | otherwise  = SettingsFrame <$> settings num id
  where
    num = payloadLength `div` 6
    isNotValid = payloadLength `mod` 6 /= 0
    settings 0 builder = return $ builder []
    settings n builder = do
        rawSetting <- BI.anyWord16be
        let msettings = toSettingsKeyId rawSetting
            n' = n - 1
        case msettings of
            Nothing -> settings n' builder -- ignoring unknown one (Section 6.5.2)
            Just k  -> do
                v <- fromIntegral <$> BI.anyWord32be
                settings n' (builder. ((k,v):))

parsePushPromiseFrame :: FramePayloadParser
parsePushPromiseFrame header = parseWithPadding header $ \len ->
    PushPromiseFrame <$> streamIdentifier <*> hbf len
  where
    hbf len = B.take $ len - 4

parsePingFrame :: FramePayloadParser
parsePingFrame FrameHeader{..}
  | payloadLength /= 8 = fail frameSizeError
  | otherwise          = PingFrame <$> B.take 8

parseGoAwayFrame :: FramePayloadParser
parseGoAwayFrame FrameHeader{..} =
    GoAwayFrame <$> streamIdentifier <*> ecid <*> debug
  where
    ecid = toErrorCodeId <$> BI.anyWord32be
    debug = B.take $ payloadLength - 8

parseWindowUpdateFrame :: FramePayloadParser
parseWindowUpdateFrame FrameHeader{..}
  | payloadLength /= 4 = fail frameSizeError -- not sure
  | otherwise          = WindowUpdateFrame <$> BI.anyWord32be

parseContinuationFrame :: FramePayloadParser
parseContinuationFrame FrameHeader{..} = ContinuationFrame <$> payload
  where
    payload = B.take payloadLength

parseUnknownFrame :: FrameType -> FramePayloadParser
parseUnknownFrame ftyp FrameHeader{..} = UnknownFrame ftyp <$> payload
  where
    payload = B.take payloadLength

----------------------------------------------------------------

-- | Helper function to pull off the padding if its there, and will
-- eat up the trailing padding automatically. Calls the parser func
-- passed in with the length of the unpadded portion between the
-- padding octet and the actual padding
parseWithPadding :: FrameHeader -> (Int -> B.Parser a) -> B.Parser a
parseWithPadding FrameHeader{..} p
  | padded = do
      padlen <- intFromWord8
      -- padding length consumes 1 byte.
      val <- p $ payloadLength - padlen - 1
      ignore padlen
      return val
  | otherwise = p payloadLength
  where
    padded = testPadded flags

streamIdentifier :: B.Parser StreamIdentifier
streamIdentifier = toStreamIdentifier . fromIntegral <$> BI.anyWord32be

streamIdentifier' :: B.Parser (StreamIdentifier, Bool)
streamIdentifier' = do
    n <- fromIntegral <$> BI.anyWord32be
    let !streamdId = toStreamIdentifier n
        !exclusive = testExclusive n
    return (streamdId, exclusive)

priority :: B.Parser Priority
priority = do
    (sid, excl) <- streamIdentifier'
    Priority excl sid <$> w
  where
    w = (+1) <$> intFromWord8

ignore :: Int -> B.Parser ()
ignore n = void $ B.take n

intFromWord8 :: B.Parser Int
intFromWord8 = fromIntegral <$> B.anyWord8

intFromWord16be :: B.Parser Int
intFromWord16be = fromIntegral <$> BI.anyWord16be
