<?php
/*
 * Copyright 2018 Nazar Gavaga.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace brebvix;

use MongoDB\BSON\JavascriptInterface;
use MongoDB\BulkWriteResult as BulkWriteResultAlias;
use MongoDB\Client;
use MongoDB\Collection;
use MongoDB\Database;
use MongoDB\DeleteResult;
use MongoDB\Driver\Cursor;
use MongoDB\Driver\Manager;
use MongoDB\Driver\ReadConcern;
use MongoDB\Driver\Session;
use MongoDB\Driver\WriteConcern;
use MongoDB\InsertManyResult;
use MongoDB\InsertOneResult;
use MongoDB\MapReduceResult;
use MongoDB\Model\IndexInfoIterator;
use MongoDB\Operation\Explainable;
use MongoDB\UpdateResult;
use Traversable;
use Yii;

/**
 * Class Mongo
 * @package brebvix
 */
class Mongo extends Client
{
    /** @var Collection[] $collection */
    protected static $collection = [];

    /** @var bool $_initialized */
    private static $_initialized = false;

    /** @var Manager $_manager */
    private static $_manager;

    /** @var Session $_globalSession */
    protected static $_globalSession;

    /**
     * @param array $options
     * @return Database
     */
    protected function getDatabase(array $options = []): Database
    {
        return parent::selectDatabase(Yii::$app->params['mongo']['databaseName'], $options);
    }

    /**
     * @param string $collectionName
     * @param array $options
     * @return Collection
     */
    protected function getCollection(string $collectionName, array $options = []): Collection
    {
        return parent::selectCollection(Yii::$app->params['mongo']['databaseName'], $collectionName, $options);
    }

    /**
     * @param array $options
     * @return Session
     */
    public static function getNewSession(array $options = [])
    {
        if (!self::$_initialized) {
            self::$_initialized = self::_initialize();
        }

        return self::$_manager->startSession($options);
    }

    /**
     * @return Collection|null
     */
    protected static function collection(): Collection
    {
        if (!self::$_initialized) {
            self::$_initialized = self::_initialize();
        }

        $collectionName = get_called_class()::collectionName();

        if (!isset(self::$collection[$collectionName]) || !is_object(self::$collection[$collectionName])) {
            self::$collection[$collectionName] = new Collection(
                self::$_manager,
                Yii::$app->params['mongo']['databaseName'],
                $collectionName
            );
        }

        return self::$collection[$collectionName];
    }

    /**
     * @return bool
     */
    private static function _initialize(): bool
    {
        self::$_manager = new Manager(Yii::$app->params['mongo']['connectionUrl']);

        if (is_object(self::$_manager)) {
            return true;
        }

        return false;
    }

    /**
     * @param array $pipeline
     * @param array $options
     *
     * @return Traversable|bool
     */
    public static function aggregate(array $pipeline, array $options = []): Traversable
    {
        return self::collection()->aggregate($pipeline, $options);
    }

    /**
     * @param array $operations
     * @param array $options
     * @return BulkWriteResultAlias
     */
    public static function bulkWrite(array $operations, array $options = []): BulkWriteResultAlias
    {
        return self::collection()->bulkWrite($operations, $options);
    }

    /**
     * @param array $filter
     * @param array $options
     *
     * @return int
     */
    public static function count($filter = [], array $options = []): int
    {
        return self::collection()->count($filter, self::_getOptions($options));
    }

    /**
     * @param array $filter
     * @param array $options
     * @return int
     */
    public static function countDocuments($filter = [], array $options = []): int
    {
        return self::collection()->countDocuments($filter, self::_getOptions($options));
    }

    /**
     * @param $key
     * @param array $options
     * @return string
     */
    public static function createIndex($key, array $options = []): string
    {
        return self::collection()->createIndex($key, $options);
    }

    /**
     * @param array $indexes
     * @param array $options
     * @return \string[]
     */
    public static function createIndexes(array $indexes, array $options = [])
    {
        return self::collection()->createIndexes($indexes, $options);
    }

    /**
     * @param $filter
     * @param array $options
     * @return DeleteResult
     */
    public static function deleteMany($filter, array $options = []): DeleteResult
    {
        return self::collection()->deleteMany($filter, self::_getOptions($options));
    }

    /**
     * @param $filter
     * @param array $options
     *
     * @return \MongoDB\DeleteResult|bool
     */
    public static function deleteOne($filter, array $options = []): DeleteResult
    {
        return self::collection()->deleteOne($filter, self::_getOptions($options));
    }

    /**
     * @param $fieldName
     * @param array $filter
     * @param array $options
     * @return \mixed[]
     */
    public static function distinct($fieldName, $filter = [], array $options = [])
    {
        return self::collection()->distinct($fieldName, $filter, self::_getOptions($options));
    }

    /**
     * @param array $options
     * @return array|object
     */
    public static function drop(array $options = [])
    {
        return self::collection()->drop($options);
    }

    /**
     * @param $indexName
     * @param array $options
     * @return array|object
     */
    public static function dropIndex($indexName, array $options = [])
    {
        return self::collection()->dropIndex($indexName, $options);
    }

    /**
     * @param array $options
     * @return array|object
     */
    public static function dropIndexes(array $options = [])
    {
        return self::collection()->dropIndexes($options);
    }

    /**
     * @param array $options
     * @return int
     */
    public static function EstimatedDocumentCount(array $options = []): int
    {
        return self::collection()->EstimatedDocumentCount($options);
    }

    /**
     * @param Explainable $explainable
     * @param array $options
     * @return array|object
     */
    public static function explain(Explainable $explainable, array $options = [])
    {
        return self::collection()->explain($explainable, self::_getOptions($options));
    }

    /**
     * @param array $filter
     * @param array $options
     *
     * @return \MongoDB\Driver\Cursor|bool
     */
    public static function find($filter = [], array $options = []): Cursor
    {
        return self::collection()->find($filter, self::_getOptions($options));
    }

    /**
     * @param array $filter
     * @param array $options
     *
     * @return array|null|object|bool|static
     */
    public static function findOne($filter = [], array $options = [])
    {
        return self::collection()->findOne($filter, self::_getOptions($options));
    }

    /**
     * @param $filter
     * @param array $options
     * @return array|null|object
     */
    public static function findOneAndDelete($filter, array $options = [])
    {
        return self::collection()->findOneAndDelete($filter, self::_getOptions($options));
    }

    /**
     * @param $filter
     * @param $replacement
     * @param array $options
     * @return array|null|object
     */
    public static function findOneAndReplace($filter, $replacement, array $options = [])
    {
        return self::collection()->findOneAndReplace($filter, $replacement, self::_getOptions($options));
    }

    /**
     * @param $filter
     * @param $update
     * @param array $options
     * @return array|null|object
     */
    public static function findOneAndUpdate($filter, $update, array $options = [])
    {
        return self::collection()->findOneAndUpdate($filter, $update, self::_getOptions($options));
    }

    /**
     * @return string
     */
    public static function getCollectionName(): string
    {
        return self::collection()->getCollectionName();
    }

    /**
     * @return string
     */
    public static function getDatabaseName(): string
    {
        return self::collection()->getDatabaseName();
    }

    /**
     * @return string
     */
    public static function getNamespace(): string
    {
        return self::collection()->getNamespace();
    }

    /**
     * @param array $documents
     * @param array $options
     *
     * @return InsertManyResult|bool
     */
    public static function insertMany(array $documents, array $options = []): InsertManyResult
    {
        return self::collection()->insertMany($documents, self::_getOptions($options));
    }

    /**
     * @param $document
     * @param array $options
     *
     * @return InsertOneResult|bool
     */
    public static function insertOne($document, array $options = []): InsertOneResult
    {
        return self::collection()->insertOne($document, self::_getOptions($options));

    }

    /**
     * @param array $options
     * @return IndexInfoIterator
     */
    public static function listIndexes(array $options = []): IndexInfoIterator
    {
        return self::collection()->listIndexes($options);
    }

    /**
     * @param JavascriptInterface $map
     * @param JavascriptInterface $reduce
     * @param $out
     * @param array $options
     * @return MapReduceResult
     */
    public static function mapReduce(JavascriptInterface $map, JavascriptInterface $reduce, $out, array $options = []): MapReduceResult
    {
        return self::collection()->mapReduce($map, $reduce, $out, self::_getOptions($options));
    }

    /**
     * @param $filter
     * @param $replacement
     * @param array $options
     * @return UpdateResult
     */
    public static function replaceOne($filter, $replacement, array $options = []): UpdateResult
    {
        return self::collection()->replaceOne($filter, $replacement, self::_getOptions($options));
    }

    /**
     * @param $filter
     * @param $update
     * @param array $options
     *
     * @return UpdateResult|bool
     */
    public static function updateMany($filter, $update, array $options = []): UpdateResult
    {
        return self::collection()->updateMany($filter, $update, self::_getOptions($options));
    }

    /**
     * @param $filter
     * @param $update
     * @param array $options
     *
     * @return UpdateResult|bool
     */
    public static function updateOne($filter, $update, array $options = []): UpdateResult
    {
        return self::collection()->updateOne($filter, $update, self::_getOptions($options));
    }

    /**
     * @param array $options
     * @return Collection
     */
    public static function withOptions(array $options = []): Collection
    {
        return self::collection()->withOptions($options);
    }

    /**
     * @return void
     */
    public static function startNewSession()
    {
        self::_initialize();

        self::$_globalSession = self::$_manager->startSession();
        self::$_globalSession->startTransaction([
            'readConcern' => new ReadConcern('snapshot'),
            'writeConcern' => new WriteConcern(WriteConcern::MAJORITY)
        ]);
    }

    /**
     * @return void
     */
    public static function cancelLastSession()
    {
        if (is_object(self::$_globalSession)) {
            try {
                self::$_globalSession->abortTransaction();
            } catch (\Exception $exception) {

            }

            self::$_globalSession->endSession();
        }

        self::$_globalSession = null;
    }

    /**
     * @return void
     */
    public static function commitSession()
    {
        self::$_globalSession->commitTransaction();
        self::cancelLastSession();
    }

    /**
     * @param array $options
     * @return array
     */
    private static function _getOptions(array $options = []): array
    {
        if (is_object(self::$_globalSession)) {
            $options['session'] = self::$_globalSession;
        }

        return $options;
    }
}