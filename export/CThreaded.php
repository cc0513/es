<?php
//Stackable是Threaded的一个别称，直到pthreads v.2.0.0
class CThreaded extends Threaded
{
    private $p1;
    private $p2;
    private $scroll = '30s';
    private $size = 100000;
    private $index = 'phone';

    public function __construct($p1, $p2)
    {
        $this->p1 = $p1;
        // print_r($this->p1[0]);
        $this->p2 = $p2;
    }
 
    public function run()
    {
        // 计算开始时间
        $s = Extend::microtime_float();
        // 实例化es
        if (extension_loaded("pthreads"))
        {
            require_once CURRENT_DIRECTORY.'/vendor/autoload.php';
        }
        // $cl = Elasticsearch\ClientBuilder::create()->setHosts(['192.168.1.104:9200', '192.168.1.103:9200'])->build();
        $cl = Elasticsearch\ClientBuilder::create()->setHosts(['192.168.1.103:9200'])->build();
        // 获取手机号
        $s = Extend::microtime_float();
        $body = [
                "query" => [
                    "constant_score" => [
                        "filter" => [
                            "bool" => [
                                "must" => [
                                    [
                                        "range" => [
                                            "num" => [
                                                "gte" => $this->p1[0],
                                                "lt" => $this->p1[1]
                                            ]
                                        ]
                                    ]
                                ]
                                // ,
                                // "must_not" => [
                                //     [
                                //         "exists" => [
                                //             "field" => "taoqizhi"
                                //         ]
                                //     ]
                                // ]
                            ]
                        ]
                    ]
                ],
                "sort" => ["_doc"]// 不考虑排序
            ];
        if (isset($this->path[3]))
        {
            $body['query']['constant_score']['filter']['bool']['must'][] = [
                                                                                "term" => [
                                                                                    "sex" => $this->path[3]
                                                                                ]
                                                                         ];
        }
        $params = [
            "scroll" => $this->scroll,          // how long between scroll requests. should be small!
            "size" => $this->size,               // how many results *per shard* you want back，调整过大，需要修改相应参数
            "index" => $this->index,
            "body" => $body
        ];
        $res = $cl->search($params);
        // return;
        Extend::log($res['_scroll_id']);
        $temp_path = $this->p1[2];
        $fp = fopen('d:/ex_csv/'.$temp_path.'_1.csv', 'a+');
        $i = 0;
        $j = 1;
        while (isset($res['hits']['hits']) && count($res['hits']['hits']) > 0)
        {
            // **
            // Do your work here, on the $res['hits']['hits'] array
            // **
            ++$i;
            $res_bulk = $res['hits']['hits'];
            foreach ($res_bulk as $key => $value)
            {
                fputcsv($fp, [$value['_source']['num']]);
            }
            // When done, get the new scroll_id
            // You must always refresh your _scroll_id!  It can change sometimes
            $scroll_id = $res['_scroll_id'];
            // Execute a Scroll request and repeat
            $res = $cl->scroll([
                    "scroll_id" => $scroll_id,  //...using our previously obtained _scroll_id
                    "scroll" => "30s"           // and the same timeout window
                ]
            );
            // 显式清除滚动 - 调用失败
            // Extend::log('显式删除-'.$scroll_id);
            // $cl->clearScroll([
            //     'custom' => [
            //         "scroll_id" => $scroll_id
            //     ]
            // ]);
            // Extend::dump_var($res);
            if($i % 100 == 0)
            {
                // ob_flush();
                // flush();
                fclose($fp);
                ++$j;
                $fp = fopen('d:/ex_csv/'.$temp_path.'_'.$j.'.csv', 'a+');
            }
        }
        if (IS_RESOURCE($fp))
        {
            fclose($fp);
        }
        // 计算结束时间
        $e = Extend::microtime_float();
        $return['path'] = $this->p1[2];
        $return['spend_time'] = ($e-$s)."秒";
        Extend::log($return);
    }
}