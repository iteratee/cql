{-# LANGUAGE CPP                 #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE TypeApplications #-}

-- | A tuple represents the types of multiple cassandra columns. It is used
-- to check that column-types match.
module Database.CQL.Protocol.Tuple
    ( Tuple
    , check
    , count
    , rowKey
    , store
    , tuple
    , Row
    , mkRow
    , fromRow
    , columnTypes
    , rowLength
    ) where

import Control.Applicative
import Control.Monad
import qualified Data.ByteString.Lazy as BSL
import Data.Functor.Identity
import Data.Int (Int32)
import Data.Serialize (Get, Putter, Serialize (..), putLazyByteString, runPutLazy)
import Data.Vector (Vector, (!?))
import Data.Word
import Database.CQL.Protocol.Class
import Database.CQL.Protocol.Codec (getValue, putValue, putValueLength)
import Database.CQL.Protocol.Tuple.TH
import Database.CQL.Protocol.Types
import Prelude

import qualified Data.Vector as Vec

-- Row ----------------------------------------------------------------------

-- | A row is a vector of 'Value's.
data Row = Row
    { types  :: ![ColumnType]
    , values :: !(Vector Value)
    } deriving (Eq, Show)

-- | Convert a row element.
fromRow :: Cql a => Int -> Row -> Either String a
fromRow i r =
    case values r !? i of
        Nothing -> Left "out of bounds access"
        Just  v -> fromCql v

mkRow :: [(Value, ColumnType)] -> Row
mkRow xs = let (v, t) = unzip xs in Row t (Vec.fromList v)

rowLength :: Row -> Int
rowLength r = Vec.length (values r)

columnTypes :: Row -> [ColumnType]
columnTypes = types

-- Tuples -------------------------------------------------------------------

-- Database.CQL.Protocol.Tuple does not export 'PrivateTuple' but only
-- 'Tuple' effectively turning 'Tuple' into a closed type-class.
class PrivateTuple a where
    count :: Tagged a Int
    check :: Tagged a ([ColumnType] -> [ColumnType])
    tuple :: Version -> [ColumnType] -> Get a
    store :: Version -> Putter a
    rowKey :: Version -> [Int32] -> Putter a

class PrivateTuple a => Tuple a

-- Manual instances ---------------------------------------------------------

instance PrivateTuple () where
    count     = Tagged 0
    check     = Tagged $ const []
    tuple _ _ = return ()
    store _   = const $ return ()
    rowKey _ _ _ = pure ()

instance Tuple ()

instance Cql a => PrivateTuple (Identity a) where
    count     = Tagged 1
    check     = Tagged $ typecheck [untag (ctype :: Tagged a ColumnType)]
    tuple v _ = Identity <$> element v ctype
    store v (Identity a) = do
        put (1 :: Word16)
        putValueLength v (toCql a)
    rowKey _ [] _ = pure ()
    rowKey v [0] (Identity x) = putValue v (toCql x)
    rowKey _ ks _ = error $ "unexpected rowKey indices: " ++ show ks

instance Cql a => Tuple (Identity a)

instance PrivateTuple Row where
    count     = Tagged (-1)
    check     = Tagged $ const []
    tuple v t = Row t . Vec.fromList <$> mapM (getValue v . MaybeColumn) t
    store v r = do
        put (fromIntegral (rowLength r) :: Word16)
        Vec.mapM_ (putValueLength v) (values r)
    rowKey _ [] _ = pure ()
    rowKey v [i] r = putValue v $ values r Vec.! fromIntegral i
    rowKey v is r =
      forM_ is $ \i -> do
        -- putValue always prepends the length as 32-bit integer. That isn't
        -- appropriate for the top level values in a row key.
        let component = BSL.drop 4 $ runPutLazy . putValue v $ values r Vec.! fromIntegral i
        put (fromIntegral @_ @Word16 $ BSL.length component)
        putLazyByteString component
        put (0 :: Word8)

instance Tuple Row

-- Implementation helpers ---------------------------------------------------

element :: Cql a => Version -> Tagged a ColumnType -> Get a
element v t = getValue v (untag t) >>= either fail return . fromCql

typecheck :: [ColumnType] -> [ColumnType] -> [ColumnType]
typecheck rr cc = if checkAll (===) rr cc then [] else rr
  where
    checkAll f as bs = and (zipWith f as bs)

    checkField (a, b) (c, d) = a == c && b === d

    TextColumn       === VarCharColumn    = True
    VarCharColumn    === TextColumn       = True
    (MaybeColumn  a) === b                = a === b
    (ListColumn   a) === (ListColumn   b) = a === b
    (SetColumn    a) === (SetColumn    b) = a === b
    (MapColumn  a b) === (MapColumn  c d) = a === c && b === d
    (UdtColumn a as) === (UdtColumn b bs) = a == b && checkAll checkField as bs
    (TupleColumn as) === (TupleColumn bs) = checkAll (===) as bs
    a                === b                = a == b

genInstances 48
