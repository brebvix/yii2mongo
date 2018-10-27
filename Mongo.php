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
    protected static $collection = null;
    protected $collectionName = null;

    /**
     * Mongo constructor.
     */
    public function __construct()
    {
        parent::__construct(Yii::$app->params['mongo']['connectionUrl']);
    }

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
     * @return Collection|null
     */
    protected static function collection(): Collection
    {
        if (!is_object(self::$collection)) {
            $client = new Client();

            self::$collection = $client->selectCollection(
                Yii::$app->params['mongo']['databaseName'],
                get_called_class()::collectionName()
            );
        }

        return self::$collection;
    }

    /**
     * @param array $pipeline
     * @param array $options
     * @return \Traversable
     */
    public static function aggregate(array $pipeline, array $options = []): \Traversable
    {
        return self::collection()->aggregate($pipeline, $options);
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
     * @return int
     */
    public static function count($filter = [], array $options = []): int
    {
        return self::collection()->count($filter, $options);
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
     * @return \MongoDB\DeleteResult
     */
    public static function deleteOne($filter, array $options = []): DeleteResult
    {
        return self::collection()->deleteOne($filter, $options);
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
     * @return \MongoDB\Driver\Cursor
     */
    public static function find($filter = [], array $options = []): Cursor
    {
        return self::collection()->find($filter, $options);
    }

    /**
     * @param array $filter
     * @param array $options
     * @return array|null|object
     */
    public static function findOne($filter = [], array $options = [])
    {
        return self::collection()->findOne($filter, $options);
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
     * @return \MongoDB\InsertManyResult
     */
    public static function insertMany(array $documents, array $options = []): InsertManyResult
    {
        return self::collection()->insertMany($documents, $options);
    }

    /**
     * @param $document
     * @param array $options
     * @return \MongoDB\InsertOneResult
     */
    public static function insertOne($document, array $options = []): InsertOneResult
    {
        return self::collection()->insertOne($document, $options);
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
     * @return UpdateResult
     */
    public static function updateMany($filter, $update, array $options = []): UpdateResult
    {
        return self::collection()->updateMany($filter, $update, $options);
    }

    /**
     * @param $filter
     * @param $update
     * @param array $options
     * @return \MongoDB\UpdateResult
     */
    public static function updateOne($filter, $update, array $options = []): UpdateResult
    {
        return self::collection()->updateOne($filter, $update, $options);
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