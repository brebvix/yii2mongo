# Yii2 MongoDB
An extension for using the MongoDB library in a style similar to ActiveRecord.
## Installation

```
composer require brebvix/yii2mongo
```

####Add to the configuration file *params-local.php*:
```php
'mongo' => [
    'connectionUrl' => 'mongodb://<username>:<password>@<host>:<port>',
    'databaseName' => '<database_name>',
],
```

Documentation: https://docs.mongodb.com/php-library/current/tutorial/

## Example

### Model class
```php
<?php
use brebvix\Mongo;

class UserModel extends Mongo
{
    /**
    * @return string
    */
    public static function collectionName(): string
    {
        return 'users';
    }
    
    //Usage in model:
    
    public static function getAuthorizedUsersCount(): int
    {
        return self::count([
            'authorized' => true
        ]);
    }
}
```

### Use outside the model:
```php
<?php
$count = UserModel::getAuthorizedUsersCount();
echo "Authorized users count: $count";

// OR

$count = UserModel::count([
    'authorized' => true
]);
echo "Authorized users count: $count";
```