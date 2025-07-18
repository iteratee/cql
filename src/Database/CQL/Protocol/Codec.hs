{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE CPP               #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Database.CQL.Protocol.Codec
    ( encodeByte
    , decodeByte

    , encodeSignedByte
    , decodeSignedByte

    , encodeShort
    , decodeShort

    , encodeSignedShort
    , decodeSignedShort

    , encodeInt
    , decodeInt

    , encodeString
    , decodeString

    , encodeLongString
    , decodeLongString

    , encodeBytes
    , decodeBytes

    , encodeShortBytes
    , decodeShortBytes

    , encodeUUID
    , decodeUUID

    , encodeList
    , decodeList

    , encodeMap
    , decodeMap

    , encodeMultiMap
    , decodeMultiMap

    , encodeSockAddr
    , decodeSockAddr

    , encodeConsistency
    , decodeConsistency

    , encodeOpCode
    , decodeOpCode

    , encodePagingState
    , decodePagingState

    , decodeKeyspace
    , decodeTable
    , decodeColumnType
    , decodeQueryId

    , putValue
    , getValuePrefix
    , getValueLength
    , putValueLength
    , putLazyByteString
    ) where

import Control.Applicative
import Control.Monad
import Data.Bits
import Data.ByteString (ByteString)
import Data.Decimal
import Data.Int
import Data.IP
#ifdef INCOMPATIBLE_VARINT
import Data.List (unfoldr)
#else
import Data.List (foldl')
#endif
import Data.Text (Text)
import Data.UUID (UUID)
import Data.Word
import Data.Persist hiding (decode, encode)
import Database.CQL.Protocol.Types
import Network.Socket (SockAddr (..), PortNumber)
import Prelude

import qualified Data.ByteString         as B
import qualified Data.ByteString.Lazy    as LB
import qualified Data.Text.Encoding      as T
import qualified Data.Text.Lazy          as LT
import qualified Data.Text.Lazy.Encoding as LT
import qualified Data.UUID               as UUID

type Putter a = a -> Put ()

getLazyByteString :: Int -> Get LB.ByteString
getLazyByteString n = LB.fromStrict <$!> getByteString n

getText :: Int -> Get Text
getText n = T.decodeUtf8 <$!> getByteString n

putLazyByteString :: LB.ByteString -> Put ()
putLazyByteString lbs = do
  forM_ (LB.toChunks lbs) $ \c ->
    putByteString c

------------------------------------------------------------------------------
-- Byte

encodeByte :: Putter Word8
encodeByte = put

decodeByte :: Get Word8
decodeByte = get

------------------------------------------------------------------------------
-- Signed Byte

encodeSignedByte :: Putter Int8
encodeSignedByte = put

decodeSignedByte :: Get Int8
decodeSignedByte = get

------------------------------------------------------------------------------
-- Short

encodeShort :: Putter Word16
encodeShort = putBE

decodeShort :: Get Word16
decodeShort = getBE

------------------------------------------------------------------------------
-- Signed Short

encodeSignedShort :: Putter Int16
encodeSignedShort = putBE

decodeSignedShort :: Get Int16
decodeSignedShort = getBE

------------------------------------------------------------------------------
-- Int

encodeInt :: Putter Int32
encodeInt = putBE

decodeInt :: Get Int32
decodeInt = getBE

------------------------------------------------------------------------------
-- String

encodeString :: Putter Text
encodeString = encodeShortBytes . T.encodeUtf8

decodeString :: Get Text
decodeString = T.decodeUtf8 <$> decodeShortBytes

------------------------------------------------------------------------------
-- Long String

encodeLongString :: Putter LT.Text
encodeLongString = encodeBytes . LT.encodeUtf8

decodeLongString :: Get LT.Text
decodeLongString = do
    n <- getBE :: Get Int32
    LT.decodeUtf8 <$> getLazyByteString (fromIntegral n)

------------------------------------------------------------------------------
-- Bytes

encodeBytes :: Putter LB.ByteString
encodeBytes bs = do
    putBE (fromIntegral (LB.length bs) :: Int32)
    putLazyByteString bs

decodeBytes :: Get (Maybe LB.ByteString)
decodeBytes = do
    n <- getBE :: Get Int32
    if n < 0
        then return Nothing
        else Just <$> getLazyByteString (fromIntegral n)

------------------------------------------------------------------------------
-- Short Bytes

encodeShortBytes :: Putter ByteString
encodeShortBytes bs = do
    putBE (fromIntegral (B.length bs) :: Word16)
    putByteString bs

decodeShortBytes :: Get ByteString
decodeShortBytes = do
    n <- getBE :: Get Word16
    getByteString (fromIntegral n)

------------------------------------------------------------------------------
-- UUID

encodeUUID :: Putter UUID
encodeUUID = putLazyByteString . UUID.toByteString

decodeUUID :: Get UUID
decodeUUID = do
    uuid <- UUID.fromByteString <$> getLazyByteString 16
    maybe (fail "decode-uuid: invalid") return uuid

------------------------------------------------------------------------------
-- String List

encodeList :: Putter [Text]
encodeList sl = do
    putBE (fromIntegral (length sl) :: Word16)
    mapM_ encodeString sl

decodeList :: Get [Text]
decodeList = do
    n <- getBE :: Get Word16
    replicateM (fromIntegral n) decodeString

------------------------------------------------------------------------------
-- String Map

encodeMap :: Putter [(Text, Text)]
encodeMap m = do
    putBE (fromIntegral (length m) :: Word16)
    forM_ m $ \(k, v) -> encodeString k >> encodeString v

decodeMap :: Get [(Text, Text)]
decodeMap = do
    n <- getBE :: Get Word16
    replicateM (fromIntegral n) ((,) <$> decodeString <*> decodeString)

------------------------------------------------------------------------------
-- String Multi-Map

encodeMultiMap :: Putter [(Text, [Text])]
encodeMultiMap mm = do
    putBE (fromIntegral (length mm) :: Word16)
    forM_ mm $ \(k, v) -> encodeString k >> encodeList v

decodeMultiMap :: Get [(Text, [Text])]
decodeMultiMap = do
    n <- getBE :: Get Word16
    replicateM (fromIntegral n) ((,) <$> decodeString <*> decodeList)

------------------------------------------------------------------------------
-- Inet Address

encodeSockAddr :: Putter SockAddr
encodeSockAddr (SockAddrInet p a) = do
    put @Word8 4
    putHE a
    putBE @Word32 (fromIntegral p)
encodeSockAddr (SockAddrInet6 p _ (a, b, c, d) _) = do
    put @Word8 16
    putBE a
    putBE b
    putBE c
    putBE d
    putBE @Word32 (fromIntegral p)
encodeSockAddr (SockAddrUnix _) = error "encode-socket: unix address not supported"
#if MIN_VERSION_network(2,6,1) && !MIN_VERSION_network(3,0,0)
encodeSockAddr (SockAddrCan _) = error "encode-socket: can address not supported"
#endif

decodeSockAddr :: Get SockAddr
decodeSockAddr = do
    n <- get @Word8
    case n of
        4  -> do
            i <- getIPv4
            p <- getPort
            return $ SockAddrInet p i
        16 -> do
            i <- getIPv6
            p <- getPort
            return $ SockAddrInet6 p 0 i 0
        _  -> fail $ "decode-socket: unknown: " ++ show n
  where
    getPort :: Get PortNumber
    getPort = fromIntegral <$!> getBE @Word32

    getIPv4 :: Get Word32
    getIPv4 = getHE

    getIPv6 :: Get (Word32, Word32, Word32, Word32)
    getIPv6 = (,,,) <$> getBE <*> getBE <*> getBE <*> getBE

------------------------------------------------------------------------------
-- Consistency

encodeConsistency :: Putter Consistency
encodeConsistency Any         = encodeShort 0x00
encodeConsistency One         = encodeShort 0x01
encodeConsistency Two         = encodeShort 0x02
encodeConsistency Three       = encodeShort 0x03
encodeConsistency Quorum      = encodeShort 0x04
encodeConsistency All         = encodeShort 0x05
encodeConsistency LocalQuorum = encodeShort 0x06
encodeConsistency EachQuorum  = encodeShort 0x07
encodeConsistency Serial      = encodeShort 0x08
encodeConsistency LocalSerial = encodeShort 0x09
encodeConsistency LocalOne    = encodeShort 0x0A

decodeConsistency :: Get Consistency
decodeConsistency = decodeShort >>= mapCode
      where
        mapCode 0x00 = return Any
        mapCode 0x01 = return One
        mapCode 0x02 = return Two
        mapCode 0x03 = return Three
        mapCode 0x04 = return Quorum
        mapCode 0x05 = return All
        mapCode 0x06 = return LocalQuorum
        mapCode 0x07 = return EachQuorum
        mapCode 0x08 = return Serial
        mapCode 0x09 = return LocalSerial
        mapCode 0x0A = return LocalOne
        mapCode code = fail $ "decode-consistency: unknown: " ++ show code

------------------------------------------------------------------------------
-- OpCode

encodeOpCode :: Putter OpCode
encodeOpCode OcError         = encodeByte 0x00
encodeOpCode OcStartup       = encodeByte 0x01
encodeOpCode OcReady         = encodeByte 0x02
encodeOpCode OcAuthenticate  = encodeByte 0x03
encodeOpCode OcOptions       = encodeByte 0x05
encodeOpCode OcSupported     = encodeByte 0x06
encodeOpCode OcQuery         = encodeByte 0x07
encodeOpCode OcResult        = encodeByte 0x08
encodeOpCode OcPrepare       = encodeByte 0x09
encodeOpCode OcExecute       = encodeByte 0x0A
encodeOpCode OcRegister      = encodeByte 0x0B
encodeOpCode OcEvent         = encodeByte 0x0C
encodeOpCode OcBatch         = encodeByte 0x0D
encodeOpCode OcAuthChallenge = encodeByte 0x0E
encodeOpCode OcAuthResponse  = encodeByte 0x0F
encodeOpCode OcAuthSuccess   = encodeByte 0x10

decodeOpCode :: Get OpCode
decodeOpCode = decodeByte >>= mapCode
  where
    mapCode 0x00 = return OcError
    mapCode 0x01 = return OcStartup
    mapCode 0x02 = return OcReady
    mapCode 0x03 = return OcAuthenticate
    mapCode 0x05 = return OcOptions
    mapCode 0x06 = return OcSupported
    mapCode 0x07 = return OcQuery
    mapCode 0x08 = return OcResult
    mapCode 0x09 = return OcPrepare
    mapCode 0x0A = return OcExecute
    mapCode 0x0B = return OcRegister
    mapCode 0x0C = return OcEvent
    mapCode 0x0D = return OcBatch
    mapCode 0x0E = return OcAuthChallenge
    mapCode 0x0F = return OcAuthResponse
    mapCode 0x10 = return OcAuthSuccess
    mapCode word = fail $ "decode-opcode: unknown: " ++ show word

------------------------------------------------------------------------------
-- ColumnType

decodeColumnType :: Get ColumnType
decodeColumnType = decodeShort >>= toType
  where
    toType 0x0000 = CustomColumn <$> decodeString
    toType 0x0001 = return AsciiColumn
    toType 0x0002 = return BigIntColumn
    toType 0x0003 = return BlobColumn
    toType 0x0004 = return BooleanColumn
    toType 0x0005 = return CounterColumn
    toType 0x0006 = return DecimalColumn
    toType 0x0007 = return DoubleColumn
    toType 0x0008 = return FloatColumn
    toType 0x0009 = return IntColumn
    toType 0x000A = return TextColumn
    toType 0x000B = return TimestampColumn
    toType 0x000C = return UuidColumn
    toType 0x000D = return VarCharColumn
    toType 0x000E = return VarIntColumn
    toType 0x000F = return TimeUuidColumn
    toType 0x0010 = return InetColumn
    toType 0x0011 = return DateColumn
    toType 0x0012 = return TimeColumn
    toType 0x0013 = return SmallIntColumn
    toType 0x0014 = return TinyIntColumn
    toType 0x0020 = ListColumn  <$> (decodeShort >>= toType)
    toType 0x0021 = MapColumn   <$> (decodeShort >>= toType) <*> (decodeShort >>= toType)
    toType 0x0022 = SetColumn   <$> (decodeShort >>= toType)
    toType 0x0030 = do
        _ <- decodeString -- Keyspace (not used by this library)
        t <- decodeString -- Type name
        UdtColumn t <$> do
            n <- fromIntegral <$> decodeShort
            replicateM n ((,) <$> decodeString <*> (decodeShort >>= toType))
    toType 0x0031 = TupleColumn <$> do
        n <- fromIntegral <$> decodeShort
        replicateM n (decodeShort >>= toType)
    toType other  = fail $ "decode-type: unknown: " ++ show other

------------------------------------------------------------------------------
-- Paging State

encodePagingState :: Putter PagingState
encodePagingState (PagingState s) = encodeBytes s

decodePagingState :: Get (Maybe PagingState)
decodePagingState = fmap PagingState <$> decodeBytes

------------------------------------------------------------------------------
-- Value

putValue :: Version -> Putter Value
putValue _ (CqlBoolean False) = put @Word8 0
putValue _ (CqlBoolean True) = put @Word8 1
putValue _ (CqlCustom x) = putLazyByteString x
putValue _ (CqlInt x) = putBE x
putValue _ (CqlBigInt x) = putBE x
putValue _ (CqlFloat x) = putBE x
putValue _ (CqlDouble x) = putBE x
putValue V4 (CqlSmallInt x) = putBE x
putValue _ v@(CqlSmallInt _) = error $ "putValue: smallint: " ++ show v
putValue V4 (CqlTinyInt x) = put @Int8 x
putValue _ v@(CqlTinyInt _) = error $ "putValue: tinyint: " ++ show v
putValue _ (CqlVarInt x) = integer2bytes x
putValue _ (CqlDecimal x) = do
    putBE @Int32 (fromIntegral $ decimalPlaces x)
    integer2bytes (decimalMantissa x)
putValue _ (CqlText x) = putByteString $ T.encodeUtf8 x
putValue _ (CqlAscii x) = putByteString $ T.encodeUtf8 x
putValue _ (CqlInet x) = case x of
    IPv4 i -> putBE $ fromIPv4w i
    IPv6 i -> do
      let (a, b, c, d) = fromIPv6w i
      putBE a >> putBE b >> putBE c >> putBE d
putValue _ (CqlUuid x) = encodeUUID x
putValue _ (CqlTimeUuid x) = encodeUUID x
putValue _ (CqlTimestamp x) = putBE x
putValue _ (CqlBlob x) = putLazyByteString x
putValue _ (CqlCounter x) = putBE x
putValue _ (CqlMaybe Nothing) = error "putValue: Nothing"
putValue ver (CqlMaybe (Just x)) = putValue ver x
putValue ver (CqlUdt x) = mapM_ (putValueLength ver . snd) x
putValue ver (CqlList x) = do
    putBE @Int32 (fromIntegral (length x))
    mapM_ (putValueLength ver) x
putValue ver (CqlSet x) = do
    putBE @Int32 (fromIntegral (length x))
    mapM_ (putValueLength ver) x
putValue ver (CqlMap x) = do
    putBE @Int32 (fromIntegral (length x))
    forM_ x $ \(k, val) -> putValueLength ver k >> putValueLength ver val
putValue ver (CqlTuple x) = mapM_ (putValueLength ver) x
putValue V4 (CqlDate x) = putBE x
putValue _ v@(CqlDate _) = error $ "putValueLength: date: " ++ show v
putValue V4 (CqlTime x) = putBE x
putValue _ v@(CqlTime _) = error $ "putValueLength: time: " ++ show v

putValueLength :: Version -> Putter Value
putValueLength _ (CqlCustom x) = do
  putBE @Int32 (fromIntegral $ LB.length x)
  putLazyByteString x
putValueLength ver val@(CqlBoolean _) = putBE @Int32 1 >> putValue ver val
putValueLength _ (CqlInt x) = putBE @Int32 4 >> putBE x
putValueLength _ (CqlBigInt x) = putBE @Int32 8 >> putBE x
putValueLength _ (CqlFloat x) = putBE @Int32 4 >> putBE x
putValueLength _ (CqlDouble x) = putBE @Int32 8 >> putBE x
putValueLength _ (CqlText x) = do
  let xBS = T.encodeUtf8 x
  putBE @Int32 (fromIntegral $ B.length xBS)
  putByteString xBS
putValueLength _ (CqlUuid x) = putBE @Int32 16 >> encodeUUID x
putValueLength _ (CqlTimeUuid x) = putBE @Int32 16 >> encodeUUID x
putValueLength _ (CqlTimestamp x) = putBE @Int32 8 >> putBE x
putValueLength ver (CqlAscii x) = putValueLength ver (CqlText x)
putValueLength _ (CqlBlob x) = do
  putBE @Int32 (fromIntegral $ LB.length x)
  putLazyByteString x
putValueLength _ (CqlCounter x) = putBE @Int32 8 >> putBE x
putValueLength ver val@(CqlInet x) = case x of
    IPv4 _ -> putBE @Int32 4 >> putValue ver val
    IPv6 _ -> putBE @Int32 16 >> putValue ver val
putValueLength V4   (CqlDate x)     = putBE @Int32 4 >> putBE x
putValueLength _  v@(CqlDate _)     = error $ "putValueLength: date: " ++ show v
putValueLength V4   (CqlTime x)     = putBE @Int32 8 >> putBE x
putValueLength _  v@(CqlTime _)     = error $ "putValueLength: time: " ++ show v
putValueLength V4   (CqlSmallInt x) = putBE @Int32 2 >> putBE x
putValueLength _  v@(CqlSmallInt _) = error $ "putValueLength: smallint: " ++ show v
putValueLength V4   (CqlTinyInt x)  = putBE @Int32 1 >> putBE x
putValueLength _  v@(CqlTinyInt _)  = error $ "putValueLength: tinyint: " ++ show v
putValueLength _ (CqlMaybe Nothing)  = putBE @Int32 (-1)
putValueLength v (CqlMaybe (Just x)) = putValueLength v x
putValueLength ver val = withLength32 $ putValue ver val

getValueLength :: Version -> ColumnType -> Get Value
getValueLength ver colType@(MaybeColumn _) = do
  mLen <- getBE @Int32
  if mLen < -1
    then fail $ "getNative: MaybeColumn too small: " ++ show mLen
    else getValuePrefix ver colType mLen
getValueLength v t = withReadPrefix (getValuePrefix v t)

getValuePrefix :: Version -> ColumnType -> Int32 -> Get Value
getValuePrefix v (ListColumn t) listLength = CqlList <$!> getList listLength (do
    len <- decodeInt
    replicateM (fromIntegral len) (getValueLength v t))
getValuePrefix v (SetColumn t) setLength = CqlSet <$!> getList setLength (do
    len <- decodeInt
    replicateM (fromIntegral len) (getValueLength v t))
getValuePrefix v (MapColumn t u) mapLength = CqlMap <$!> getList mapLength (do
    len <- decodeInt
    replicateM (fromIntegral len) ((,) <$!> getValueLength v t <*> getValueLength v u))
getValuePrefix v (TupleColumn t) tLength = withPrefix tLength $ CqlTuple <$!> mapM (getValueLength v) t
getValuePrefix v (MaybeColumn t) mLength = do
    if mLength < 0
        then return (CqlMaybe Nothing)
        else CqlMaybe . Just <$!> getValuePrefix v t mLength
getValuePrefix _ (CustomColumn _) l = CqlCustom <$!> getLazyByteString (fromIntegral l)
getValuePrefix _ BooleanColumn l = assertLength 1 l >> CqlBoolean . (/= 0) <$!> get @Word8
getValuePrefix _ IntColumn     l  = assertLength 4 l >> CqlInt <$!> getBE
getValuePrefix _ BigIntColumn  l  = assertLength 8 l >> CqlBigInt <$!> getBE
getValuePrefix _ FloatColumn   l  = assertLength 4 l >> CqlFloat  <$!> getBE
getValuePrefix _ DoubleColumn  l  = assertLength 8 l >> CqlDouble <$!> getBE
getValuePrefix _ TextColumn    l  = CqlText <$!> getText (fromIntegral l)
getValuePrefix _ VarCharColumn l  = CqlText <$!> getText (fromIntegral l)
getValuePrefix _ AsciiColumn   l  = CqlAscii <$!> getText (fromIntegral l)
getValuePrefix _ BlobColumn    l  = CqlBlob <$!> getLazyByteString (fromIntegral l)
getValuePrefix _ UuidColumn    l  = assertLength 16 l >> CqlUuid <$!> decodeUUID
getValuePrefix _ TimeUuidColumn l = assertLength 16 l >> CqlTimeUuid <$!> decodeUUID
getValuePrefix _ TimestampColumn l = assertLength 8 l >> CqlTimestamp <$!> getBE
getValuePrefix _ CounterColumn  l = assertLength 8 l >> CqlCounter <$!> getBE
getValuePrefix _ InetColumn l = CqlInet <$!> do
    case l of
        4  -> IPv4 . toIPv4w <$!> getBE
        16 -> do
            a <- getBE
            b <- getBE
            c <- getBE
            d <- getBE
            pure . IPv6 $! toIPv6w (a, b, c, d)
        n  -> fail $ "getNative: invalid Inet length: " ++ show n
getValuePrefix V4 DateColumn   l = assertLength 4 l >> CqlDate <$!> getBE
getValuePrefix _  DateColumn   _ = fail "getNative: date type"
getValuePrefix V4 TimeColumn   l = assertLength 8 l >> CqlTime <$!> getBE
getValuePrefix _  TimeColumn   _ = fail "getNative: time type"
getValuePrefix V4 SmallIntColumn l = assertLength 2 l >> CqlSmallInt <$!> getBE
getValuePrefix _  SmallIntColumn _ = fail "getNative: smallint type"
getValuePrefix V4 TinyIntColumn l  = assertLength 1 l >> CqlTinyInt <$!> get
getValuePrefix _  TinyIntColumn _ = fail "getNative: tinyint type"
getValuePrefix _  VarIntColumn  l = withPrefix l $ CqlVarInt <$!> bytes2integer
getValuePrefix _  DecimalColumn l = withPrefix l $ do
    x <- getBE @Int32
    CqlDecimal . Decimal (fromIntegral x) <$> bytes2integer
getValuePrefix v (UdtColumn _ x) l = withPrefix l $ CqlUdt <$!> do
    let (n, t) = unzip x
    zip n <$> mapM (getValueLength v) t

assertLength :: Int32 -> Int32 -> Get ()
assertLength expected provided | expected == provided = pure ()
                               | otherwise =
  fail $ "getNative: Expecting length: " ++ show expected ++ ", but got: " ++ show provided

getList :: Int32 -> Get [a] -> Get [a]
getList n m = do
    if n < 0 then return []
             else withPrefix n m

withPrefix :: Int32 -> Get a -> Get a
withPrefix prefixLength = getPrefix (fromIntegral prefixLength)

withReadPrefix :: (Int32 -> Get a) -> Get a
withReadPrefix p = do
    n <- fromIntegral <$!> getBE @Int32
    when (n < 0) $
        fail $ "withReadPrefix: null (length = " ++ show n ++ ")"
    p n

remainingBytes :: Get ByteString
remainingBytes = remaining >>= getByteString . fromIntegral

withLength32 :: Put () -> Put ()
withLength32 basePut = do
  lengthSlot <- reserveSize @Int32
  basePut
  resolveSizeExclusiveBE lengthSlot

#ifdef INCOMPATIBLE_VARINT

-- 'integer2bytes' and 'bytes2integer' implementations are taken
-- from cereal's instance declaration of 'Serialize' for 'Integer'
-- except that no distinction between small and large integers is made.
-- Cf. to LICENSE for copyright details.
integer2bytes :: Putter Integer
integer2bytes n = do
    put sign
    put (unroll (abs n))
  where
    sign = fromIntegral (signum n) :: Word8

    unroll :: Integer -> [Word8]
    unroll = unfoldr step
      where
        step 0 = Nothing
        step i = Just (fromIntegral i, i `shiftR` 8)

bytes2integer :: Get Integer
bytes2integer = do
    sign  <- get
    bytes <- get
    let v = roll bytes
    return $! if sign == (1 :: Word8) then v else - v
  where
    roll :: [Word8] -> Integer
    roll = foldr unstep 0
      where
        unstep b a = a `shiftL` 8 .|. fromIntegral b

#else

integer2bytes :: Putter Integer
integer2bytes n
    | n == 0  = put @Word8 0x00
    | n == -1 = put @Word8 0xFF
    | n <  0  = do
        let bytes = explode (-1) n
        unless (head bytes >= 0x80) $
            put @Word8 0xFF
        mapM_ (put @Word8) bytes
    | otherwise = do
        let bytes = explode 0 n
        unless (head bytes < 0x80) $
            put @Word8 0x00
        mapM_ (put @Word8) bytes

explode :: Integer -> Integer -> [Word8]
explode x n = loop n []
  where
    loop !i !acc
        | i == x    = acc
        | otherwise = loop (i `shiftR` 8) (fromIntegral i : acc)

bytes2integer :: Get Integer
bytes2integer = do
    msb   <- get @Word8
    bytes <- B.unpack <$> remainingBytes
    if msb < 0x80
        then return (implode (msb:bytes))
        else return (- (implode (map complement (msb:bytes)) + 1))

implode :: [Word8] -> Integer
implode = foldl' fun 0
  where
    fun i b = i `shiftL` 8 .|. fromIntegral b

#endif
------------------------------------------------------------------------------
-- Various

decodeKeyspace :: Get Keyspace
decodeKeyspace = Keyspace <$> decodeString

decodeTable :: Get Table
decodeTable = Table <$> decodeString

decodeQueryId :: Get (QueryId k a b)
decodeQueryId = QueryId <$> decodeShortBytes
