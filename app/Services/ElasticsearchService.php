<?php

namespace App\Services;

use Elastic\Elasticsearch\ClientBuilder;
use Exception;
use Http\Promise\Promise;
use Illuminate\Support\Facades\Log;

class ElasticsearchService
{
    protected $client;

    public function __construct()
    {
        $this->client = ClientBuilder::create()->setHosts([config('services.elk.host_ip')])
            ->setBasicAuthentication(config('services.elk.username'), config('services.elk.password'))
            ->setSSLVerification(false)->build();
    }

    /**
     *
     * Summery: Test the connection to the Elasticsearch server.
     *
     * @return string
     */
    public function testConnection(): string
    {
        try {
            $response = $this->client->ping();

            return $response ? 'Connection successful' : 'Connection failed';

        } catch (Exception $exception) {
            Log::error($exception->getMessage());
        }
    }

    /**
     *
     * Summery: Create an index in Elasticsearch with the given name and default settings.
     *
     * @param string $indexName
     * @return Promise
     */
    public function createIndex(string $indexName): Promise
    {
        try {
            $params = [
                'index' => $indexName,
                'body' => [
                    'settings' => [
                        'number_of_shards' => 1,
                        'number_of_replicas' => 0
                    ],
                    'mappings' => [
                        'properties' => [
                            'title' => [
                                'type' => 'text'
                            ],
                            'content' => [
                                'type' => 'text'
                            ]
                        ]
                    ]
                ]
            ];

            return $this->client->indices()->create($params);

        } catch (Exception $exception) {
            Log::error($exception->getMessage());
        }
    }

    /**
     *
     * Summery: Populate an index with the given data.
     *
     * @param string $indexName
     * @param array $data
     * @return Promise
     */
    public function populateIndex(string $indexName, array $data): Promise
    {
        try {
            $params = [
                'index' => $indexName,
                'body' => $data
            ];

            return $this->client->index($params);

        } catch (Exception $exception) {
            Log::error($exception->getMessage());
        }
    }

    /**
     *
     * Summery: Verify if a document with the given ID exists in the specified index.
     *
     * @param string $index
     * @param string $id
     * @return array
     */
    private function verifyExists(string $index, string $id): array
    {
        try {
            $data = $this->client->search(['index' => $index,
                'body' => ['query' => ['bool' => ['must' => ['term' => ['id' => $id]]]]]
            ]);

            return $data['hits']['hits'];

        } catch (Exception $exception) {
            Log::error($exception->getMessage());
        }
    }

    /**
     * Perform a bulk index operation with the given data.
     *
     * @param string $indexName
     * @param array $data
     * @return array
     * @throws Exception
     */
    public function bulkIndexData(string $indexName, array $data): array
    {
        try {
            $params = ['body' => []];

            foreach ($data as $item) {
                $elastic_prop = $this->verifyExists($indexName, $item['id']);

                if (!count($elastic_prop)) {
                    $params['body'][] = [
                        'create' => [
                            '_index' => $indexName
                        ]
                    ];

                    $params['body'][] = $item;
                } else {
                    $params['body'][] = [
                        'update' => [
                            '_index' => $indexName,
                            '_id' => $elastic_prop[0]['_id'],
                        ]
                    ];

                    $params['body'][] = ['doc' => $item];
                }
            }

            if (!empty($params['body'])) {
                $response = $this->client->bulk($params);

                if (isset($response['errors']) && $response['errors']) {
                    throw new \Exception('Bulk operation failed: ' . json_encode($response['items']));
                }
            }

            return ['message' => 'Bulk operation successful'];

        } catch (Exception $exception) {
            Log::error($exception->getMessage());
        }
    }

    /**
     *
     * Summery: Get paginated data from the specified index.
     *
     * @param string $indexName
     * @param int $page
     * @param int $pageSize
     * @return array|string
     */
    public function getPaginatedIndexData(string $indexName, int $page = 1, int $pageSize = 10): array|string
    {
        try {
            $from = ($page - 1) * $pageSize;

            $params = [
                'index' => $indexName,
                'body' => [
                    'from' => $from,
                    'size' => $pageSize,
                    'query' => [
                        'match_all' => new \stdClass()
                    ]
                ]
            ];

            $response = $this->client->search($params);

            if (isset($response['hits']['hits'])) {
                return [
                    'total' => $response['hits']['total']['value'],
                    'data' => $response['hits']['hits'],
                    'current_page' => $page,
                    'per_page' => $pageSize
                ];
            }

            return 'No documents found';

        } catch (Exception $exception) {
            Log::error($exception->getMessage());
        }
    }

    /**
     *
     * Summery: Get data from the specified index by document ID.
     *
     * @param string $indexName
     * @param string $id
     * @return array|string
     */
    public function getIndexData(string $indexName, string $id): array|string
    {
        try {
            $params = [
                'index' => $indexName,
                'body' => [
                    'query' => [
                        'match' => [
                            '_id' => $id
                        ]
                    ]
                ]
            ];

            $response = $this->client->search($params);

            if (isset($response['hits']['hits'][0])) {
                return $response['hits']['hits'][0];
            }

            return 'Document not found';

        } catch (Exception $exception) {
            Log::error($exception->getMessage());
        }
    }
}
