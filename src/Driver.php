<?php

/**
 * Iqomp\Model PDO Driver
 * @package iqomp/model-hyperf-db
 * @version 2.2.0
 */

namespace Iqomp\ModelHyperfDb;

use Hyperf\DbConnection\Db;
use Hyperf\Database\Exception\QueryException;

class Driver implements \Iqomp\Model\DriverInterface
{
    protected $model;
    protected $table;
    protected $connections;
    protected $config;
    protected $chains;

    protected $last_connection;

    protected $last_error;
    protected $has_error;

    protected $last_id;

    protected function getDb(
        string $target = 'read',
        array $where = [],
        array $order = [],
        int $rpp = 0,
        int $page = 1
    ){
        $db = Db::table($this->getTable());

        // sort
        if ($order) {
            $db = $this->putOrder($db, $order);
        }

        // pager
        if ($rpp > 0) {
            $db = $this->putPagination($db, $rpp, $page);
        }

        // where
        if ($where) {
            $db = $this->putWhere($db, $where);
        }

        return $db;
    }

    protected function exec($db, string $method, array $args=[])
    {
        $this->has_error = false;

        try {
            $result = call_user_func_array([$db, $method], $args);
        } catch (QueryException $e) {
            $this->has_error = true;
            $this->last_error = $e->getMessage();
            return null;
        }

        return $result;
    }

    protected function makeFieldSelect(string $field): string
    {
        if (false !== strstr($field, '.')) {
            return $field;
        }

        return $this->getTable() . '.' . $field;
    }

    protected function makeWhere(string $prefix, string $method)
    {
        if ($prefix === 'or') {
            $method = ucfirst($method);
        }

        return $prefix . $method;
    }

    protected function putOrder($db, $order)
    {
        foreach ($order as $field => $direction) {
            $db = $db->orderBy($field, ($direction ? 'asc' : 'desc'));
        }

        return $db;
    }

    protected function putPagination($db, $rpp, $page)
    {
        $db = $db->take($rpp);

        $skip = 0;
        if ($page > 1) {
            $page--;
            $skip = $page * $rpp;
            if ($skip) {
                $db = $db->skip($skip);
            }
        }

        return $db;
    }

    protected function putWhere($db, array $where, string $or = '')
    {
        foreach ($where as $field => $value) {
            if (false !== strstr($field, '.')) {
                $fields = explode('.', $field);
                $table_name = $fields[0];
                $field_name = $fields[1];

                $chain = $this->chains[$table_name] ?? null;
                if ($chain) {
                    $j_method = 'join';
                    $j_model = $chain['model'];

                    if (isset($chain['type'])) {
                        $j_method = strtolower($chain['type']) . 'Join';
                    }

                    $j_table = $j_model::getTable();
                    $j_self  = $this->getTable() . '.' . $chain['self'];
                    $j_child = $j_table . '.' . $chain['children'];

                    $db = $db->$j_method($j_table, $j_child, '=', $j_self);

                    $field = $j_table . '.' . $field_name;
                }
            }

            $field = $this->makeFieldSelect($field);

            if ($field === '$or' || $field === '$and') {
                $method = $this->makeWhere($or, 'where');
                $db = $db->$method(function($qry) use ($field, $value) {
                    $or = $field === '$or' ? 'or' : '';
                    foreach ($value as $where) {
                        $qry = $this->putWhere($qry, $where, $or);
                    }
                });
            } else {
                if (is_array($value)) {
                    $cond  = $value[0] ?? null;
                    $count = count($value);

                    if ($cond === '__op' && $count === 3) {
                        $db = $this->putWhereOp($db, $field, $value, $or);
                    } elseif ($cond === '__like' && $count > 1) {
                        $db = $this->putWhereLike($db, $field, $value, $or);
                    } elseif ($cond === '__between' && $count === 3) {
                        $db = $this->putWhereBetween($db, $field, $value, $or);
                    } else {
                        $method = $this->makeWhere($or, 'whereIn');
                        $db = $db->$method($field, $value);
                    }
                } else {
                    $method = $this->makeWhere($or, 'where');
                    $db = $db->$method($field, $value);
                }
            }
        }

        return $db;
    }

    protected function putWhereBetween(
        $db,
        string $field,
        array $value,
        string $or = ''
    ) {
        $method = $this->makeWhere($or, 'whereBetween');
        return $db->$method($field, [$value[1], $value[2]]);
    }

    protected function putWhereLike(
        $db,
        string $field,
        array $options,
        string $or = ''
    ) {
        $value    = $options[1];
        $side     = $options[2] ?? 'both';
        $negation = $options[3] ?? null;
        $operator = 'LIKE';

        if ($negation === 'NOT') {
            $operator = 'NOT LIKE';
        }

        if (in_array($side, ['left', 'right', 'both'])) {
            $str_val = is_string($value);
            $value   = (array)$value;

            $l_str   = in_array($side, ['left', 'both'])  ? '%' : '';
            $r_str   = in_array($side, ['right', 'both']) ? '%' : '';

            foreach ($value as &$val) {
                $val = $l_str . $val . $r_str;
            }
            unset($val);

            if ($str_val) {
                $value = $value[0];
            }
        }

        if (is_string($value)) {
            $method = $this->makeWhere($or, 'where');
            return $db->$method($field, $operator, $value);
        }

        $method = $this->makeWhere($or, 'where');
        $db = $db->$method(function($query) use ($field, $operator, $negation, $value) {
            if ($negation === 'NOT') {
                foreach ($value as $val) {
                    $query = $query->where($field, $operator, $val);
                }
            } else {
                $method = 'where';
                foreach ($value as $val) {
                    $query = $query->$method($field, $operator, $val);
                    $method = 'orWhere';
                }
            }
        });

        return $db;
    }

    protected function putWhereOp(
        $db,
        string $field,
        array $options,
        string $or = ''
    ) {
        $operator = $options[1];
        $value    = $options[2];

        if ($operator == '!=' || $operator == 'NOT IN') {
            $operator = '!=';
        }

        if (is_array($value)) {
            if ($operator == '!=') {
                $method = $this->makeWhere($or, 'whereNotIn');
                $db = $db->$method($field, $value);
            } else {
                $method = $this->makeWhere($or, 'whereIn');
                $db = $db->$method($field, $value);
            }
        } else {
            $method = $this->makeWhere($or, 'where');
            $db = $db->$method($field, $operator, $value);
        }

        return $db;
    }

    public function __construct(array $options)
    {
        $this->model       = $options['model'];
        $this->table       = $options['table'];
        $this->connections = $options['connections'];
        $this->chains      = $options['chains'];
    }

    public function avg(string $field, array $where = []): float
    {
        $db = $this->getDb('read', $where);
        return $this->exec($db, 'avg', [$field]);
    }

    public function beginTransaction(): void
    {
        Db::beginTransaction();
    }

    public function commit(): void
    {
        Db::commit();
    }

    public function count(array $where = []): int
    {
        $db = $this->getDb('read', $where);
        return $this->exec($db, 'count') ?? 0;
    }

    public function create(array $row, bool $ignore = false): ?int
    {
        $db = $this->getDb('write');

        if ($ignore) {
            $id = $this->exec($db, 'insertOrIgnore', [$row]);
        } else {
            $id = $this->exec($db, 'insertGetId', [$row]);
        }

        if (!$id) {
            return false;
        }

        $this->last_id = $id;

        return $this->last_id;
    }

    public function createMany(array $rows, bool $ignore = false): bool
    {
        $db = $this->getDb('write');

        if ($ignore) {
            return $this->exec($db, 'insertOrIgnore', [$rows]);
        } else {
            return $this->exec($db, 'insert', [$rows]);
        }

    }

    public function dec(array $fields, array $where = []): bool
    {
        foreach ($fields as $field => $value) {
            $db = $this->getDb('write', $where);
            if (!$this->exec($db, 'decrement', [$field, $value])) {
                return false;
            }
        }

        return true;
    }

    public function escape(string $str): string
    {
        $conn = $this->getConnection();
        return (string)$conn->quote($str);
    }

    public function getOne(
        array $where = [],
        array $order = ['id' => false]
    ): ?object {
        $db = $this->getDb('read', $where, $order);
        $db = $db->select($this->getTable() . '.*');
        return $this->exec($db, 'first');
    }

    public function get(
        array $where = [],
        int $rpp = 0,
        int $page = 1,
        array $order = ['id' => false]
    ): array {
        $db = $this->getDb('read', $where, $order, $rpp, $page);
        $db = $db->select($this->getTable() . '.*');
        $result = $this->exec($db, 'get');
        if (!$result) {
            return [];
        }

        return array_values($result->all());
    }

    public function getConnection(string $target = 'read')
    {
        $conn = $this->getConnectionName($target);
        return DB::connection($conn)->getPdo();
    }

    public function getConnectionName(string $target = 'read'): ?string
    {
        return $this->connections[$target]['name'];
    }

    public function getDBName(string $target = 'read'): ?string
    {
        $conn = $this->getConnectionName($target);
        return Db::connection($conn)->getDatabaseName();
    }

    public function getDriver(): ?string
    {
        return 'pdo';
    }

    public function getModel(): string
    {
        return $this->model;
    }

    public function getTable(): string
    {
        return $this->table;
    }

    public function inc(array $fields, array $where = []): bool
    {
        foreach ($fields as $field => $value) {
            $db = $this->getDb('write', $where);
            if (!$this->exec($db, 'increment', [$field, $value])) {
                return false;
            }
        }

        return true;
    }

    public function lastError(): ?string
    {
        return $this->last_error;
    }

    public function lastId(): ?int
    {
        return $this->last_id;
    }

    public function lastQuery(): ?string
    {
        $logs = DB::getQueryLog();
        if (!$logs) {
            return null;
        }

        $log  = end($logs);
        if (!isset($log['query'])) {
            return null;
        }

        $bindings = $log['bindings'];
        $query    = $log['query'];

        $query = str_replace('?', '\'%s\'', $query);
        $query = vsprintf($query, $bindings);

        return $query;
    }

    public function max(string $field, array $where = []): int
    {
        $db = $this->getDb('read', $where);
        return (int)$this->exec($db, 'max', [$field]);
    }

    public function min(string $field, array $where = []): int
    {
        $db = $this->getDb('read', $where);
        return (int)$this->exec($db, 'min', [$field]);
    }

    public function remove(array $where = []): bool
    {
        $db = $this->getDb('write', $where);
        return (bool)$this->exec($db, 'delete');
    }

    public function rollback(): void
    {
        Db::rollBack();
    }

    public function set(array $fields, array $where = []): bool
    {
        if (!$fields) {
            return true;
        }
        $db = $this->getDb('write', $where);
        return (bool)$this->exec($db, 'update', [$fields]);
    }

    public function sum(string $field, array $where = []): int
    {
        $db = $this->getDb('read', $where);
        return (int)$this->exec($db, 'sum', [$field]);
    }

    public function truncate(string $target = 'write'): bool
    {
        $db = $this->getDb('write');
        $this->exec($db, 'truncate');

        return !$this->has_error;
    }
}
