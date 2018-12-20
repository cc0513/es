<?php
// php D:\php_code\es\Import.php s
if (!isset($argv[1]) or $argv[1] != 's')
{
    exit('$argv[1] error');
}

set_time_limit(0);
ini_set('date.timezone','Asia/Shanghai');
ini_set('display_errors', 1);
error_reporting(E_ALL);
ini_set('error_log', __DIR__ . '/import_error.txt');
ini_set('memory_limit', '204800M');

define('CURRENT_DIRECTORY', __DIR__);
define('WORKERS', 25);

require_once CURRENT_DIRECTORY.'/Extend.php';

// 获取csv路径,taoqizhi不能为字符串类型
// $file_direc = 'D:/1-zhuji-test';
$file_direc = 'D:/1-zhuji-temp';
$file_names = scandir($file_direc);
unset($file_names[0], $file_names[1]);
array_multisort($file_names, SORT_ASC, SORT_NUMERIC);
foreach ($file_names as $key => $value)
{
    $temp_path_array = scandir($file_direc.'/'.$value);
    // Extend::dump_var($temp_path_array);exit;
    unset($temp_path_array[0], $temp_path_array[1]);
    foreach ($temp_path_array as $keys => $values)
    {
        $path_array[] = $file_direc.'/'.$value.'/'.$values;
    }
}
// Extend::log($path_array);
// exit;

// 执行多线程
$s = Extend::microtime_float();
bulk($path_array);
$e = Extend::microtime_float();
Extend::log("多线程耗时：".($e-$s)."秒");
exit;

/**
 * 多线程导入数据到es
 */
function bulk($chunk_path_array)
{
    require_once CURRENT_DIRECTORY.'/extend/CsvReader.php';
    $pool = new Pool(WORKERS, 'CWorker', []);
    foreach ($chunk_path_array as $key => $value)
    {
        // Extend::log($value['path']);
        // Threaded
        $pool->submit(new CThreaded($value, $key));
    }
    //循环收集垃圾，阻塞主线程，等待子线程结束
    while($pool->collect());
    $pool->shutdown();
}

//Worker是具有持久化上下文(执行环境)的线程对象
//Worker对象start()后，会执行run()方法，run()方法执行完毕，线程也不会消亡
class CWorker extends Worker
{
    private $obj = null;
 
    public function __construct($obj = null)
    {
        // Extend::log('CWorker __construct');
        $this->$obj = $obj;
        // Extend::log(self::$obj);
        // print_r(self::$obj);
    }
 
    public function run()
    {
        // Extend::log('CWorker run');
    }
}

/**
 * es使用多线程导入数据
 */
class CThreaded extends Threaded
{
    private $p1;
    private $p2;
    // private $index = 'phone';
    private $index = 'test2';
    private $type = 'my_type';
 
    public function __construct($p1, $p2)
    {
        $this->p1 = $p1;
        $this->p2 = $p2;
    }

    public function run()
    {
        // 计算开始时间
        $s = Extend::microtime_float();

        // 执行
        $data = $this->get_csv();
        $this->import_data($data, count($data));
        // $data = $this->get_txt();
        // $this->update_data($data);
        // $this->get_data($data);

        // 计算结束时间
        $e = Extend::microtime_float();
        $return['path'] = $this->p1;
        $return['spend_time'] = ($e-$s)."秒";
        Extend::log($return);
    }

    /**
     * 获取csv数据
     * @return array
     */
    public function get_csv()
    {
        $m_size = Extend::convert(memory_get_usage(true));
        Extend::log('读取csv开始内存：'.$m_size);
        $csv_reader = new CsvReader($this->p1);
        $line_number = $csv_reader->get_lines();
        // Extend::log($line_number);
        $data = $csv_reader->get_data($line_number+1);
        $m_size = Extend::convert(memory_get_usage(true));
        Extend::log('读取csv结束内存：'.$m_size);
        if($this->p2 == 0) unset($data[0]);
        // Extend::log($data);
        return $data;
    }

    /**
     * 获取txt数据
     * @return array
     */
    public function get_txt()
    {
        $m_size = Extend::convert(memory_get_usage(true));
        Extend::log('读取txt开始内存：'.$m_size);
        $str = file_get_contents($this->path);//将整个文件内容读入到一个字符串中
        $data = explode("\r\n", $str);//转换成数组
        array_pop($data);
        $m_size = Extend::convert(memory_get_usage(true));
        Extend::log('读取txt结束内存：'.$m_size);
        // Extend::log($data);
        return $data;
    }

    /**
     * 比较记录 - 不使用，影响插入文档效率
     * @return boolean
     */
    public function compare_record($cl, $insert_data)
    {
        // $t = microtime(true);
        $params = [
            'index' => $this->index,
            'type' => $this->type,
            'body' => [
                'query' => [
                    'constant_score' => [ //非评分模式执行
                        'filter' => [ //过滤器，不会计算相关度，速度快
                            'term' => [ // 精确查找，不支持多个条件
                                '_id' => $insert_data[1]
                            ]
                        ]
                    ]
                ]
            ]
        ];
        $res = $cl->search($params);
        // $e = microtime(true);
        // Extend::log("比较记录耗时：".($e-$t));
        // Extend::log('compare：'.print_r($res, true));
        if ($res['hits']['total'])
        {
            if (strtotime($insert_data[5]) > strtotime($res['hits']['hits'][0]['_source']['timestamp']))
            {
                return true;
            }
            return false;
        }
        return true;
    }

    /**
     * 导入数据
     */
    public function import_data($data, $count)
    {
        require_once CURRENT_DIRECTORY.'/vendor/autoload.php';
        $cl = Elasticsearch\ClientBuilder::create()->setHosts(['192.168.1.103:9200'])->build();
        $params = ['body' => []];
        $j = 0;
        for ($i=$count; $i > 0; $i--)
        {
            if (!isset($data[$i][1]) || !isset($data[$i][2]) || $data[$i][2] > 2147483647)
            {
                continue;
            }
            ++$j;
            $params['body'][] = [
                'index' => [
                    '_index' => $this->index,
                    '_type' => $this->type,
                    '_id' => $data[$i][1]
                ]
            ];
            $params['body'][] = [
                'num' => $data[$i][1],
                'taoqizhi' => $data[$i][2],
                'sex' => $data[$i][3],
                'name' => $data[$i][4],
                'head' => $data[$i][5],
                'timestamp' => $data[$i][6]
            ];
            // $params['body'][] = [
            //     'num' => $data[$i][1],
            //     'timestamp' => $data[$i][2]
            // ];
            // Every 1000 documents stop and send the bulk request
            if ($j % 2000 == 0)
            {
                $res = $cl->bulk($params);
                // erase the old bulk request
                $params = ['body' => []];
                // unset the bulk response when you are done to save memory
                unset($res);
            }
        }
        // Send the last batch if it exists
        // Extend::log($params);
        if (!empty($params['body']))
        {
            $res = $cl->bulk($params);
        }
    }

    /**
     * 更新数据
     */
    public function update_data($data)
    {
        require_once CURRENT_DIRECTORY.'/vendor/autoload.php';
        $cl = Elasticsearch\ClientBuilder::create()->setHosts(['192.168.1.103:9200'])->build();
        $params = ['body' => []];
        $j = 0;
        foreach ($data as $key => $value)
        {
            // if (!isset($value)) {
            //     continue;
            // }
            if (!isset($value[2]) || $value[2]=="" || $value[2] > 2147483647)
            {
                continue;
            }
            if (!isset($value[5]) || $value[5]=="")
            {
                $value[5] = 0;
            }
            ++$j;
            $params['body'][] = [
                'update' => [
                    '_index' => $this->index,
                    '_type' => $this->type,
                    '_id' => $value[1]
                ]
            ];
            $params['body'][] = [
                'doc' => [
                    'taoqizhi' => intval($value[2]),
                    'timestamp' => $value[6],
                    'evaluate2' => intval($value[5])
                ],
                'doc_as_upsert' => true
            ];
            // Every 1000 documents stop and send the bulk request
            if ($j % 1000 == 0)
            {
                $res = $cl->bulk($params);
                // erase the old bulk request
                $params = ['body' => []];
                // unset the bulk response when you are done to save memory
                unset($res);
            }
        }
        // Send the last batch if it exists
        // Extend::log($params);
        if (!empty($params['body']))
        {
            $res = $cl->bulk($params);
        }
    }

    /**
     * 抽取数据，导入csv
     */
    public function get_data($data)
    {
        require_once CURRENT_DIRECTORY.'/vendor/autoload.php';
        $cl = Elasticsearch\ClientBuilder::create()->setHosts(['192.168.1.103:9200'])->build();
        $fp = fopen('d:/ex_csv/'.$this->p2.'.csv', 'a+');
        foreach ($data as $key => $value)
        {
            if (strlen($value) != 11)
            {
                continue;
            }
            $params = [
                'index' => $this->index,
                'type' => $this->type,
                'body' => [
                    'query' => [
                        'constant_score' => [ //非评分模式执行
                            'filter' => [ //过滤器，不会计算相关度，速度快
                                "bool" => [
                                    "must" => [
                                        [
                                            "term" => [
                                                "num" => $value
                                            ]
                                        ]
                                    ]
                                    ,
                                    "must_not" => [
                                        [
                                            "exists" => [
                                                "field" => "sex"
                                            ]
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ];
            $res = $cl->search($params);
            if ($res['hits']['total'] == 1)
            {
                fputcsv($fp, [$value]);
            }
        }
        fclose($fp);
        // Extend::log($res);
    }
}
