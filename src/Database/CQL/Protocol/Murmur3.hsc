{-# LANGUAGE CApiFFI #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE TypeApplications #-}

module Database.CQL.Protocol.Murmur3 
  ( murmur3HashKey 
  ) where

import Data.ByteString (ByteString)
import Data.ByteString.Unsafe (unsafeUseAsCStringLen)
import Data.Int (Int64)
import Data.Word (Word8)
import Foreign.C.Types (CSize (..))
import Foreign.Marshal.Alloc (allocaBytesAligned)
import Foreign.Storable (Storable (..))
import Foreign.Ptr (Ptr, castPtr)
import System.IO.Unsafe (unsafePerformIO)

foreign import capi unsafe "murmur_hash_3_cassandra.h MurmurHash3_x64_128_cassandra"
  c_Murmur3HashCassandra :: Ptr Word8 -> CSize -> Int64 -> Ptr Int64 -> IO ()

normalize :: Int64 -> Int64
normalize x | x == minBound = maxBound
            | otherwise = x

murmur3HashKey :: ByteString -> Int64
murmur3HashKey key = normalize . unsafePerformIO $
  unsafeUseAsCStringLen key $ \(p, l) ->
    allocaBytesAligned (sizeOf @Int64 0 * 2) (alignment @Int64 0) $ \hPtr -> do
      c_Murmur3HashCassandra (castPtr p) (fromIntegral l) 0 hPtr
      peek hPtr
    
