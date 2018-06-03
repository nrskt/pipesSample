module Lib
    ( running
    , asyncRunning
    , concRunning
    ) where

import           Control.Concurrent.Async  (Concurrently (..), async,
                                            runConcurrently, wait)
import           Control.Lens              (view, (^.), _1, _2, _3)
import           Data.Extensible           (type (>:), Associate, Record,
                                            hsequence, nil, (<:), (<@=>), (@=))
import           Data.Hashable             (Hashable)
import qualified Data.HashMap.Strict       as HM
import           Data.List                 (sort)
import           Data.Monoid               (Monoid (..), (<>))
import           Data.Text.Prettyprint.Doc (pretty)
import           Pipes                     (Producer, each, runEffect, (<-<))
import           Pipes.Group               (FreeT, folds, groupsBy, maps)
import qualified Pipes.Prelude             as P
import           Pipes.Safe                (SafeT, runSafeT)


-- |
-- 型定義

-- |
-- Aggregateできることを表す型
newtype Aggregatable a = Aggregatable a deriving (Show)

class IsAggregation a where
    type HashMapKey a :: *

    toAggregatable :: a -> Aggregatable a
    toAggregatable = Aggregatable

    fromAggregatable :: Aggregatable a -> a
    fromAggregatable (Aggregatable a) = a

    toHashMap :: a -> HM.HashMap (HashMapKey a) a

class ToModel a where
    type ReturnType a :: *
    toModel :: a -> ReturnType a

type SampleName = String
type SampleId = Int
type ValueAName = String
type ValueBName = String
type ValueBItem = Int
type Sort = Int

type SampleDomainModel = Record
    [ "sampleId" >: SampleId
    , "name" >: SampleName
    , "valueA" >: ValueA
    , "valueB" >: ValueB
    ]

type ValueA = Record
    [ "sampleId" >: SampleId
    , "name" >: ValueAName
    , "value" >: [ValueAItem]
    ]

type ValueAItem = Record
    '[ "val" >: Int
     ]

instance IsAggregation ValueA where
    type HashMapKey ValueA = SampleId
    toHashMap model = HM.singleton (model ^. #sampleId) model

instance Monoid (Aggregatable ValueA) where
    mempty = toAggregatable $ #sampleId @= 0 <: #name @= "" <: #value @= [item] <: nil
        where
            item :: ValueAItem
            item = #val @= 0 <: nil
    (Aggregatable x) `mappend` (Aggregatable y) =
        toAggregatable $
               #sampleId @= y ^. #sampleId
            <: #name @= y ^. #name
            <: #value @= x ^. #value <> y ^. #value
            <: nil

type ValueB = Record
    [ "sampleId" >: SampleId
    , "name" >: ValueBName
    , "value1" >: ValueBItem
    ]

instance IsAggregation ValueB where
    type HashMapKey ValueB = SampleId
    toHashMap model = HM.singleton (model ^. #sampleId) model

instance Monoid (Aggregatable ValueB) where
    mempty = toAggregatable $ #sampleId @= 0 <: #name @= "" <: #value1 @= 0 <: nil
    (Aggregatable x) `mappend` (Aggregatable y) =
        toAggregatable $
               #sampleId @= y ^. #sampleId
            <: #name @= y ^. #name
            <: #value1 @= x ^. #value1 + y ^. #value1
            <: nil

type SampleDomainTable = (SampleId, SampleName)

-- |
-- ValueBの型定義
type ValueATable = (SampleId, ValueAName, Int)

instance ToModel ValueATable where
    type ReturnType ValueATable = ValueA

    toModel r = #sampleId @= r^._1 <: #name @= r^._2 <: #value @= [valueItem] <: nil
        where
            valueItem = #val @= r^._3 <: nil

-- |
-- ValueBの型定義
type ValueBTable = (SampleId, ValueBName, ValueBItem, Sort)

instance ToModel ValueBTable where
    type ReturnType ValueBTable = ValueB

    toModel r = #sampleId @= r^._1 <: #name @= r^._2 <: #value1 @= r^._3 <: nil


-- |
-- ダミーデータ

sampleDomainTable :: [SampleDomainTable]
sampleDomainTable = [(x, "test" <> show x) | x <- [1..100000]]

valueATable :: [ValueATable]
valueATable = [ (x, "valueA " <> show x, x * 10) | x <- sort $ concat $ replicate 5 [1..100000]]

valueBTable :: [ValueBTable]
valueBTable = [ (x, "valueB " <> show x, x * 10, 1) | x <- sort $ concat $ replicate 5 [1..100000]]

-- |
-- Query Mock

-- |
-- Generic parts functions

compareSampleId :: (Associate "sampleId" SampleId r)
                => Aggregatable (Record r)
                -> Aggregatable (Record r)
                -> Bool
compareSampleId (Aggregatable r1) (Aggregatable r2) = r1 ^. #sampleId == r2 ^. #sampleId

toAggregatedHashMap :: (Monad m, Eq k, Hashable k, IsAggregation v)
                    => Producer (HM.HashMap k v) (SafeT m) ()
                    -> (SafeT m) (HM.HashMap k v)
toAggregatedHashMap = P.fold HM.union HM.empty id

-- |
-- SampleDomainModelの組み立て
sampleDomainProducer :: (Monad m) => Producer SampleDomainTable (SafeT m) ()
sampleDomainProducer = each sampleDomainTable

makeSampleDomain :: ValueAMap
                 -> ValueBMap
                 -> SampleDomainTable
                 -> Maybe SampleDomainModel
makeSampleDomain aMap bMap table = hsequence
       ( #sampleId <@=> pure (table ^. _1)
        <: #name <@=> pure (table ^. _2)
        <: #valueA <@=> HM.lookup (table ^. _1) aMap
        <: #valueB <@=> HM.lookup (table ^. _1) bMap
        <: nil
       )

pipeLine :: ValueAMap -> ValueBMap -> IO [Maybe SampleDomainModel]
pipeLine a b = do
    r <- runSafeT $ P.toListM (P.map (makeSampleDomain a b) <-< sampleDomainProducer)
    return r


-- |
-- ValueAをDatabaseから取得しHashMapまでを作成する
type ValueAMap = HM.HashMap SampleId ValueA

aggregatedValueA :: (Monad m) => (SafeT m) ValueAMap
aggregatedValueA =
    toAggregatedHashMap (P.map (toHashMap . fromAggregatable) <-< folds (<>) mempty id groupStreams)
    where
        groupStreams :: Monad m => FreeT (Producer (Aggregatable ValueA) (SafeT m)) (SafeT m) ()
        groupStreams = view (groupsBy compareSampleId) source

        source :: (Monad m) => Producer (Aggregatable ValueA) (SafeT m) ()
        source = P.map toAggregatable <-< P.map toModel <-< each valueATable

-- |
-- ValueBをDatabaseから取得しHashMapまでを作成する
type ValueBMap = HM.HashMap SampleId ValueB

aggregatedValueB :: (Monad m) => (SafeT m) ValueBMap
aggregatedValueB =
    toAggregatedHashMap (P.map (toHashMap . fromAggregatable) <-< folds (<>) mempty id groupStreams)
    where
        groupStreams :: Monad m => FreeT (Producer (Aggregatable ValueB) (SafeT m)) (SafeT m) ()
        groupStreams = view (groupsBy compareSampleId) source

        source :: (Monad m) => Producer (Aggregatable ValueB) (SafeT m) ()
        source = P.map toAggregatable <-< P.map toModel <-< each valueBTable

-- |
-- 実行部分
-- 23.59s user 6.75s system 221% cpu 13.687 total
running :: IO ()
running = do
    valueAMap <- runSafeT aggregatedValueA
    valueBMap <- runSafeT aggregatedValueB
    r <- pipeLine valueAMap valueBMap
    print $ pretty r

-- async
asyncRunning :: IO ()
asyncRunning = do
    valueAMap <- async $ runSafeT aggregatedValueA
    valueBMap <- async $ runSafeT aggregatedValueB
    valueAMap' <- wait valueAMap
    valueBMap' <- wait valueBMap
    r <- pipeLine valueAMap' valueBMap'
    print $ pretty r

-- concurrently
concRunning :: IO ()
concRunning = do
    (valAMap, valBMap)
        <- runConcurrently $ (,)
            <$> Concurrently (runSafeT aggregatedValueA)
            <*> Concurrently (runSafeT aggregatedValueB)
    r <- pipeLine valAMap valBMap
    print $ pretty r
