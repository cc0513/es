<?php
// php D:\php_code\es\MExport.php s2
if (!isset($argv[1]) or $argv[1] != 's2')
{
    exit('$argv[1] error');
}

// 允许脚本运行的最大的执行时间，单位为秒。如果设置为0（零），则没有时间方面的限制。
set_time_limit(0);
ini_set('date.timezone','Asia/Shanghai');
ini_set('display_errors', 1);
error_reporting(E_ALL);
ini_set('error_log', __DIR__ . '/export_error.txt');
ini_set('memory_limit', '204800M');

define('CURRENT_DIRECTORY', __DIR__);
define('WORKERS', 25);

if (!extension_loaded("pthreads"))
{
    require_once CURRENT_DIRECTORY.'/vendor/autoload.php';
}
require_once CURRENT_DIRECTORY.'/Extend.php';
require_once CURRENT_DIRECTORY.'/export/CWorker.php';
require_once CURRENT_DIRECTORY.'/export/CThreaded.php';

// 定义查询条件
// $ex_arr = [
//     [13000000000,13003000000,'t1'],
//     [13003000000,13005000000,'t2'],
//     [13005000000,13007000000,'t3'],
//     [13007000000,13009000000,'t4'],
//     [13009000000,13012000000,'t5'],
//     [13012000000,13014000000,'t6'],
//     [13014000000,13016000000,'t7'],
//     [13016000000,13018000000,'t8'],
//     [13018000000,13020000000,'t9'],
//     [13020000000,13022000000,'t10'],
// ];
// 号码间隔200W，共50份，刚好取得1个号段，130号段共25718463
$ori_mobile = 13000000000;
$step = 2000000;
$loop = 25;
// $step = 200000;
// $loop = 3;
for ($i=0; $i < $loop; $i++)
{
    $ex_arr[] = [$ori_mobile + $step * $i, $ori_mobile + $step * ($i+1), 't'.$i];
}
// Extend::log($ex_arr);
// exit;

// 执行多线程
$s = Extend::microtime_float();
bulk($ex_arr);
$e = Extend::microtime_float();
Extend::log("多线程耗时：".($e-$s)."秒");
exit;

/**
 * 多线程导入数据到es
 */
function bulk($chunk_path_array)
{
    $pool = new Pool(WORKERS, 'CWorker', []);
    // Extend::log(get_declared_classes());
    // print_r($pool);
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