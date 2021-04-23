<?php

/**
 * Module default config
 * @package iqomp/model-hyperf-db
 * @version 2.0.0
 */

namespace Iqomp\ModelHyperfDb;

class ConfigProvider
{
    public function __invoke()
    {
        return [
            'model' => [
                'drivers' => [
                    'mysql' => 'Iqomp\\ModelHyperfDb\\Driver'
                ]
            ]
        ];
    }
}
