{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell   #-}
{-# LANGUAGE TypeFamilies      #-}

{-# OPTIONS_GHC -fno-warn-orphans #-}

module Tests where

import Control.Applicative hiding (many)
import Control.Arrow
import Database.CQL.Protocol
import Database.CQL.Protocol.Internal
import Data.Decimal
import Data.Int
import Data.IP
import Data.Maybe
import Data.Persist (runGet, runPut)
import Data.Text (Text)
import Data.Time
import Data.Time.Clock.POSIX
import Data.UUID
import Test.QuickCheck
import Test.Tasty
import Test.Tasty.QuickCheck
import Prelude

import qualified Data.ByteString.Lazy as LB
import qualified Data.Text            as T
import Test.Tasty.HUnit (testCase, Assertion, assertEqual)
import Data.Functor.Identity (Identity(..))
import Data.Function ((&))


getPutIdentity :: Val v -> Property
getPutIdentity Val{..} =
    let t = typeof value
        x = runGet (getValueLength version t) (runPut (putValueLength version value))
    in Right value === x

integralCodec :: Show a => Gen a -> ColumnType -> (a -> Value) -> Property
integralCodec g t f = forAll g $ \i ->
    let x = f i in Right x === runGet (getValueLength V3 t) (runPut (putValueLength V3 x))

toCqlFromCqlIdentity :: Value -> Property
toCqlFromCqlIdentity x@(CqlBoolean _)   = (toCql <$> (fromCql x :: Either String Bool))     === Right x
toCqlFromCqlIdentity x@(CqlInt _)       = (toCql <$> (fromCql x :: Either String Int32))    === Right x
toCqlFromCqlIdentity x@(CqlBigInt _)    = (toCql <$> (fromCql x :: Either String Int64))    === Right x
toCqlFromCqlIdentity x@(CqlSmallInt _)  = (toCql <$> (fromCql x :: Either String Int16))    === Right x
toCqlFromCqlIdentity x@(CqlTinyInt _)   = (toCql <$> (fromCql x :: Either String Int8))     === Right x
toCqlFromCqlIdentity x@(CqlFloat _)     = (toCql <$> (fromCql x :: Either String Float))    === Right x
toCqlFromCqlIdentity x@(CqlDouble _)    = (toCql <$> (fromCql x :: Either String Double))   === Right x
toCqlFromCqlIdentity x@(CqlText _)      = (toCql <$> (fromCql x :: Either String T.Text))   === Right x
toCqlFromCqlIdentity x@(CqlInet _)      = (toCql <$> (fromCql x :: Either String IP))       === Right x
toCqlFromCqlIdentity x@(CqlUuid _)      = (toCql <$> (fromCql x :: Either String UUID))     === Right x
toCqlFromCqlIdentity x@(CqlTimestamp _) = (toCql <$> (fromCql x :: Either String UTCTime))  === Right x
toCqlFromCqlIdentity x@(CqlAscii _)     = (toCql <$> (fromCql x :: Either String Ascii))    === Right x
toCqlFromCqlIdentity x@(CqlBlob _)      = (toCql <$> (fromCql x :: Either String Blob))     === Right x
toCqlFromCqlIdentity x@(CqlCounter _)   = (toCql <$> (fromCql x :: Either String Counter))  === Right x
toCqlFromCqlIdentity x@(CqlTimeUuid _)  = (toCql <$> (fromCql x :: Either String TimeUuid)) === Right x
toCqlFromCqlIdentity x@(CqlVarInt _)    = (toCql <$> (fromCql x :: Either String Integer))  === Right x
toCqlFromCqlIdentity x@(CqlDecimal _)   = (toCql <$> (fromCql x :: Either String Decimal))  === Right x
toCqlFromCqlIdentity _                  = True === True

typeof :: Value -> ColumnType
typeof (CqlBoolean _)      = BooleanColumn
typeof (CqlInt _)          = IntColumn
typeof (CqlBigInt _)       = BigIntColumn
typeof (CqlSmallInt _)     = SmallIntColumn
typeof (CqlTinyInt _)      = TinyIntColumn
typeof (CqlVarInt _)       = VarIntColumn
typeof (CqlFloat _)        = FloatColumn
typeof (CqlDecimal _)      = DecimalColumn
typeof (CqlDouble _)       = DoubleColumn
typeof (CqlText _)         = TextColumn
typeof (CqlInet _)         = InetColumn
typeof (CqlUuid _)         = UuidColumn
typeof (CqlTimestamp _)    = TimestampColumn
typeof (CqlAscii _)        = AsciiColumn
typeof (CqlBlob _)         = BlobColumn
typeof (CqlCounter _)      = CounterColumn
typeof (CqlTimeUuid _)     = TimeUuidColumn
typeof (CqlDate _)         = DateColumn
typeof (CqlTime _)         = TimeColumn
typeof (CqlMaybe Nothing)  = MaybeColumn (CustomColumn "a")
typeof (CqlMaybe (Just a)) = MaybeColumn (typeof a)
typeof (CqlList [])        = ListColumn  (CustomColumn "a")
typeof (CqlList (x:_))     = ListColumn  (typeof x)
typeof (CqlSet  [])        = SetColumn (CustomColumn "a")
typeof (CqlSet  (x:_))     = SetColumn (typeof x)
typeof (CqlMap  [])        = MapColumn (CustomColumn "a") (CustomColumn "b")
typeof (CqlMap  ((x,y):_)) = MapColumn (typeof x) (typeof y)
typeof (CqlCustom _)       = CustomColumn "a"
typeof (CqlTuple x)        = TupleColumn (map typeof x)
typeof (CqlUdt   x)        = UdtColumn "" (map (second typeof) x)

genValue :: Version -> Gen Value
genValue v = sized $ \n ->
    oneof [ gen v n
          , CqlMaybe <$> oneof [Just <$> gen v n, pure Nothing]
          ]
  where
    many   n = gen v (n `div` 2) >>= resize n . listOf  . return
    many1  n = gen v (n `div` 2) >>= resize n . listOf1 . return
    gen V3 n = oneof (v3 n)
    gen V4 n = oneof (v4 n)

    v3 0 = v3Leaf
    v3 n = v3Leaf ++
        [ CqlList  <$> many n
        , CqlSet   <$> many n
        , CqlMap   <$> (zip <$> many n <*> many n)
        , CqlTuple <$> many1 n
        ]

    v4 0 = v4Leaf ++ v3Leaf
    v4 n = v4Leaf ++ v3 n

    v3Leaf =
        [ CqlAscii     <$> arbitrary
        , CqlBigInt    <$> arbitrary
        , CqlBlob      <$> arbitrary
        , CqlBoolean   <$> arbitrary
        , CqlCounter   <$> arbitrary
        , CqlCustom    <$> arbitrary
        , CqlDouble    <$> arbitrary
        , CqlFloat     <$> arbitrary
        , CqlInet      <$> arbitrary
        , CqlInt       <$> arbitrary
        , CqlTimeUuid  <$> arbitrary
        , CqlTimestamp <$> arbitrary
        , CqlUuid      <$> arbitrary
        , CqlText      <$> arbitrary
        , CqlDecimal   <$> arbitrary
        , CqlVarInt    <$> arbitrary
        ]

    v4Leaf =
        [ CqlTime      <$> arbitrary
        , CqlDate      <$> arbitrary
        , CqlSmallInt  <$> arbitrary
        , CqlTinyInt   <$> arbitrary
        ]

data Val v = Val
    { version :: !Version
    , value   :: !Value
    } deriving Show

data V3
data V4

instance Arbitrary (Val V3) where
    arbitrary = Val V3 <$> genValue V3

instance Arbitrary (Val V4) where
    arbitrary = Val V4 <$> genValue V4

instance Arbitrary Value where
    arbitrary = genValue V4

instance Arbitrary LB.ByteString where
    arbitrary = LB.pack <$> arbitrary

instance Arbitrary T.Text where
    arbitrary = T.pack <$> arbitrary

instance Arbitrary IP where
    arbitrary = oneof
        [ IPv4 . fromHostAddress  <$> arbitrary
        , IPv6 . fromHostAddress6 <$> arbitrary
        ]

instance Bounded UUID where
    minBound = nil
    maxBound = fromJust $ fromString "ffffffff-ffff-4fff-bfff-ffffffffffff"

instance Arbitrary UUID where
    arbitrary = arbitraryBoundedRandom

instance Arbitrary UTCTime where
    arbitrary = posixSecondsToUTCTime . fromIntegral
        <$> (arbitrary :: Gen Int64)

instance Arbitrary (DecimalRaw Integer) where
    arbitrary = Decimal <$> arbitrary <*> arbitrary

-----------------------------------------------------------------------------
-- TH code generation test.

data TestRecord = TestRecord
    { testRecordA :: IP
    , testRecordB :: Text
    , testRecordC :: Int32
    } deriving Show

recordInstance ''TestRecord

-----------------------------------------------------------------------------
-- Test cases for Murmur3 partition keys

testMurmur3Vectors :: Assertion
testMurmur3Vectors = do
    assertEqual
        "Text key from murmur3 test vectors: WRfl"
        422668743870662549
        (rowKey V4 [0] (Identity ("WRfl" :: Text)) & runPut & murmur3HashKey)
    assertEqual
        "Text key from murmur3 test vectors: mrG7A06RDb04APyqtJDcSQ"
        (-6351911484294487542)
        (rowKey V4 [0] (Identity ("mrG7A06RDb04APyqtJDcSQ" :: Text)) & runPut & murmur3HashKey)
    assertEqual
        "Text key from murmur3 test vectors: P3/cwhnIkfwYgfg"
        (-794555513299321809)
        (rowKey V4 [0] (Identity ("P3/cwhnIkfwYgfg" :: Text)) & runPut & murmur3HashKey)
    assertEqual
        "Text key from murmur3 test vectors: 9mjOeUyuFA"
        8688054346336436262
        (rowKey V4 [0] (Identity ("9mjOeUyuFA" :: Text)) & runPut & murmur3HashKey)
    assertEqual
        "Text key from murmur3 test vectors: U/a/7qtkKQx7rzjVjDKscxbb"
        5768307025851008073
        (rowKey V4 [0] (Identity ("U/a/7qtkKQx7rzjVjDKscxbb" :: Text)) & runPut & murmur3HashKey)
    assertEqual
        "Text key from murmur3 test vectors: RfZfkoP0UPPez+6jQbvtTA1BwWiLVLLP"
        2204601279383471284
        (rowKey V4 [0] (Identity ("RfZfkoP0UPPez+6jQbvtTA1BwWiLVLLP" :: Text)) & runPut & murmur3HashKey)
    assertEqual
        "Text key from murmur3 test vectors: CzOkHw5RLoUvA3YyXlw4Lq8e4j8"
        (-3675581488006656363)
        (rowKey V4 [0] (Identity ("CzOkHw5RLoUvA3YyXlw4Lq8e4j8" :: Text)) & runPut & murmur3HashKey)
    assertEqual
        "Text key from murmur3 test vectors: wI6jMf/mmg"
        (-1509962630060265403)
        (rowKey V4 [0] (Identity ("wI6jMf/mmg" :: Text)) & runPut & murmur3HashKey)
    assertEqual
        "Text key from murmur3 test vectors: JI8cbVgTif6cB6+nSGaORSMHcr8bZw"
        (-6681131792718328292)
        (rowKey V4 [0] (Identity ("JI8cbVgTif6cB6+nSGaORSMHcr8bZw" :: Text)) & runPut & murmur3HashKey)
    assertEqual
        "Text key from murmur3 test vectors: mIycuRm3Z3LARmzEkDO4zoH1705rLD8"
        2637968942887083513
        (rowKey V4 [0] (Identity ("mIycuRm3Z3LARmzEkDO4zoH1705rLD8" :: Text)) & runPut & murmur3HashKey)
    assertEqual
        "Text key from murmur3 test vectors: Ctt0a606J7cSNA"
        7401972305268388745
        (rowKey V4 [0] (Identity ("Ctt0a606J7cSNA" :: Text)) & runPut & murmur3HashKey)
    assertEqual
        "Text key from murmur3 test vectors: 60rZ+HacaYtqujAXU4xOzZCVlFjOkjC9SlS4"
        (-1780712415847777487)
        (rowKey V4 [0] (Identity ("60rZ+HacaYtqujAXU4xOzZCVlFjOkjC9SlS4" :: Text)) & runPut & murmur3HashKey)
    assertEqual
        "Text key from murmur3 test vectors: jqMk6mIIn8NUxaKr4rQO3CyU"
        (-7948171348135874697)
        (rowKey V4 [0] (Identity ("jqMk6mIIn8NUxaKr4rQO3CyU" :: Text)) & runPut & murmur3HashKey)
    assertEqual
        "Text key from murmur3 test vectors: jRci7TcmwIARMXB3KsQdHS8Y1qj6izB4Pmksfh0"
        (-355136189899934876)
        (rowKey V4 [0] (Identity ("jRci7TcmwIARMXB3KsQdHS8Y1qj6izB4Pmksfh0" :: Text)) & runPut & murmur3HashKey)
    assertEqual
        "Text key from murmur3 test vectors: 9V9Olj0H"
        (-7800684702521659878)
        (rowKey V4 [0] (Identity ("9V9Olj0H" :: Text)) & runPut & murmur3HashKey)
    assertEqual
        "Text key from murmur3 test vectors: WLu4UWFyx0WaFlNPeCn4O1EFWzzzdM6akqyOdA"
        5257543896247840857
        (rowKey V4 [0] (Identity ("WLu4UWFyx0WaFlNPeCn4O1EFWzzzdM6akqyOdA" :: Text)) & runPut & murmur3HashKey)

testCompoundKeys :: Assertion
testCompoundKeys = do
    assertEqual
      "Known compound key for: []"
      (-3485513579396041028)
      (rowKey V4 [0] (Identity ([] :: [Text])) & runPut & murmur3HashKey)
    assertEqual
      "Known compound key for: [\"rules\"]"
      5696842773425782423
      (rowKey V4 [0] (Identity (["rules"] :: [Text])) & runPut & murmur3HashKey)
    assertEqual
      "Known compound key for: [\"rules\", \"scoring\", \"basic\"]"
      1132589589130033412
      (rowKey V4 [0] (Identity (["rules", "scoring", "basic"] :: [Text])) & runPut & murmur3HashKey)

tests :: TestTree
tests = testGroup "Codec"
    [ testProperty "V3: getValue . putValueLength = id" (getPutIdentity :: Val V3 -> Property)
    , testProperty "V4: getValue . putValueLength = id" (getPutIdentity :: Val V4 -> Property)
    , testProperty "toCql . fromCql = id" toCqlFromCqlIdentity
    , testGroup "Integrals"
        [ testProperty "Int Codec"     $ integralCodec (elements [-512..512]) IntColumn CqlInt
        , testProperty "BigInt Codec"  $ integralCodec (elements [-512..512]) BigIntColumn CqlBigInt
        , testProperty "Integer Codec" $ integralCodec (elements [-512..512]) VarIntColumn CqlVarInt
        ]
    , testGroup "Tokens"
        [ testCase "Murmur3 C Test Vectors" testMurmur3Vectors
        , testCase "Known Compound Keys" testCompoundKeys
        ]
    ]
