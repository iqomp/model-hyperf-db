<?php

return [
    'drivers' => [
        'pdo' => 'Iqomp\\ModelHyperfDb\\Driver'
    ],
    'connections' => [
        'default' => [
            'driver' => 'pdo',
            'configs' => [
                'master' => '-'
            ]
        ]
    ]
];
