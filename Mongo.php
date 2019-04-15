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

use MongoDB\Driver\Exception\RuntimeException;
use MongoDB\Driver\Manager;
use Traversable;
use MongoDB\BulkWriteResult;
use MongoDB\Collection;
use MongoDB\Database;
use MongoDB\DeleteResult;
use MongoDB\Driver\Cursor;
use MongoDB\InsertManyResult;
use MongoDB\InsertOneResult;
use MongoDB\MapReduceResult;
use MongoDB\Model\IndexInfoIterator;
use MongoDB\Operation\Explainable;
use MongoDB\UpdateResult;
use Yii;
use MongoDB\Client;

class Mongo extends Client
{
    protected static $collection = [];
    private static $_initialized = false;
    private static $_manager;

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
     * @return \MongoDB\MongoDB\Driver\Session
     */
    public static function getNewSession(array $options = [])
    {
        if (!self::$_initialized) {
            self::$_initialized = self::_initialize();
        }

        return  self::$_manager->startSession($options);
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
     * @param int $attemptNumber
     * @return \Traversable|bool
     */
    public static function aggregate(array $pipeline, array $options = [], int $attemptNumber = 0): Traversable
    {
        if ($attemptNumber >= 15) {
            return false;
        }

        try {
            return self::collection()->aggregate($pipeline, $options);
        } catch (\Exception $exception) {
            return self::aggregate($pipeline, $options, ++$attemptNumber);
        }
    }

    /**
     * @param array $operations
     * @param array $options
     * @return \MongoDB\BulkWriteResult
     */
    public static function bulkWrite(array $operations, array $options = []): BulkWriteResult
    {
        return self::collection()->bulkWrite($operations, $options);
    }

    /**
     * @param array $filter
     * @param array $options
     * @param int $attemptNumber
     * @return int
     */
    public static function count($filter = [], array $options = [], int $attemptNumber = 0): int
    {
        if ($attemptNumber >= 15) {
            return false;
        }

        try {
            return self::collection()->count($filter, $options);
        } catch (\Exception $exception) {
            return self::count($filter, $options, ++$attemptNumber);
        }
    }

    /**
     * @param array $filter
     * @param array $options
     * @return int
     */
    public static function countDocuments($filter = [], array $options = []): int
    {
        return self::collection()->countDocuments($filter, $options);
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
        return self::collection()->deleteMany($filter, $options);
    }

    /**
     * @param $filter
     * @param array $options
     * @param int $attemptNumber
     * @return \MongoDB\DeleteResult|bool
     */
    public static function deleteOne($filter, array $options = [], int $attemptNumber = 0): DeleteResult
    {
        if ($attemptNumber >= 15) {
            return false;
        }

        try {
            return self::collection()->deleteOne($filter, $options);
        } catch (\Exception $exception) {
            return self::deleteOne($filter, $options, ++$attemptNumber);
        }
    }

    /**
     * @param $fieldName
     * @param array $filter
     * @param array $options
     * @return \mixed[]
     */
    public static function distinct($fieldName, $filter = [], array $options = [])
    {
        return self::collection()->distinct($fieldName, $filter, $options);
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
        return self::collection()->explain($explainable, $options);
    }

    /**
     * @param array $filter
     * @param array $options
     * @param int $attemptNumber
     * @return \MongoDB\Driver\Cursor|bool
     */
    public static function find($filter = [], array $options = [], int $attemptNumber = 0): Cursor
    {
        if ($attemptNumber >= 15) {
            return false;
        }

        try {
            return self::collection()->find($filter, $options);
        } catch (\Exception $exception) {
            return self::find($filter, $options, ++$attemptNumber);
        }
    }

    /**
     * @param array $filter
     * @param array $options
     * @param int $attemptNumber
     * @return array|null|object|bool
     */
    public static function findOne($filter = [], array $options = [], int $attemptNumber = 0)
    {
        if ($attemptNumber >= 15) {
            return false;
        }

        try {
            return self::collection()->findOne($filter, $options);
        } catch (\Exception $exception) {
            return self::findOne($filter, $options, ++$attemptNumber);
        }
    }

    /**
     * @param $filter
     * @param array $options
     * @return array|null|object
     */
    public static function findOneAndDelete($filter, array $options = [])
    {
        return self::collection()->findOneAndDelete($filter, $options);
    }

    /**
     * @param $filter
     * @param $replacement
     * @param array $options
     * @return array|null|object
     */
    public static function findOneAndReplace($filter, $replacement, array $options = [])
    {
        return self::collection()->findOneAndReplace($filter, $replacement, $options);
    }

    /**
     * @param $filter
     * @param $update
     * @param array $options
     * @return array|null|object
     */
    public static function findOneAndUpdate($filter, $update, array $options = [])
    {
        return self::collection()->findOneAndUpdate($filter, $update, $options);
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
     * @param int $attemptNumber
     * @return \MongoDB\InsertManyResult|bool
     */
    public static function insertMany(array $documents, array $options = [], int $attemptNumber = 0): InsertManyResult
    {
        if ($attemptNumber >= 15) {
            return false;
        }

        try {
            return self::collection()->insterMany($documents, $options);
        } catch (\Exception $exception) {
            return self::insertMany($documents, $options, ++$attemptNumber);
        }
    }

    /**
     * @param $document
     * @param array $options
     * @param int $attemptNumber
     * @return \MongoDB\InsertOneResult|bool
     */
    public static function insertOne($document, array $options = [], int $attemptNumber = 0): InsertOneResult
    {
        if ($attemptNumber >= 15) {
            return false;
        }

        try {
            return self::collection()->insertOne($document, $options);
        } catch (\Exception $exception) {
            return self::insertOne($document, $options, ++$attemptNumber);
        }
    }

    /**
     * @param array $options
     * @return \MongoDB\Model\IndexInfoIterator
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
     * @return \MongoDB\MapReduceResult
     */
    public static function mapReduce(JavascriptInterface $map, JavascriptInterface $reduce, $out, array $options = []): MapReduceResult
    {
        return self::collection()->mapReduce($map, $reduce, $out, $options);
    }

    /**
     * @param $filter
     * @param $replacement
     * @param array $options
     * @return UpdateResult
     */
    public static function replaceOne($filter, $replacement, array $options = []): UpdateResult
    {
        return self::collection()->replaceOne($filter, $replacement, $options);
    }

    /**
     * @param $filter
     * @param $update
     * @param array $options
     * @param int $attemptNumber
     * @return UpdateResult|bool
     */
    public static function updateMany($filter, $update, array $options = [], int $attemptNumber = 0): UpdateResult
    {
        if ($attemptNumber >= 15) {
            return false;
        }

        try {
            return self::collection()->updateMany($filter, $update, $options);
        } catch (\Exception $exception) {
            return self::updateMany($filter, $update, $options, ++$attemptNumber);
        }
    }

    /**
     * @param $filter
     * @param $update
     * @param array $options
     * @param int $attemptNumber
     * @return \MongoDB\UpdateResult|bool
     */
    public static function updateOne($filter, $update, array $options = [], int $attemptNumber = 0): UpdateResult
    {
        if ($attemptNumber >= 15) {
            return false;
        }

        try {
            return self::collection()->updateOne($filter, $update, $options);
        } catch (\Exception $exception) {
            return self::updateOne($filter, $update, $options, ++$attemptNumber);
        }
    }

    /**
     * @param array $options
     * @return Collection
     */
    public static function withOptions(array $options = []): Collection
    {
        return self::collection()->withOptions($options);
    }
}