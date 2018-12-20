<?php
/**
 * 扩展方法
 */
class Extend
{
    /**
     * 打印数据
     * @param $var 要打印的数据
     * @param $type 数据类型
     * @return string
     */
    public static function dump_var($var, $type = 1)
    {
        switch ($type) {
            case 1:
                print_r($var);
                break;
            case 2:
                var_dump($var);
                break;
        }
    }

    /**
     * 转化内存单位
     * @param $size 内存大小
     */
    public static function convert($size)
    {
        $unit = ['b','kb','mb','gb','tb','pb'];
        return @round($size/pow(1024,($i=floor(log($size,1024)))),2).' '.$unit[$i]; 
    }

    /**
     * 打印日志
     * @param $data 要打印的数据
     * @param $file 日志文件名称
     */
    public static function log($data, $file = 'log.txt')
    {
        file_put_contents(CURRENT_DIRECTORY.'/'.$file, "[".date('Y-m-d H:i:s')."] " . print_r($data, true).PHP_EOL, FILE_APPEND);
    }

    /**
     * 计算微秒
     * @return float
     */
    public static function microtime_float()
    {
        list($usec, $sec) = explode(" ", microtime());
        return ((float)$usec + (float)$sec);
    }
}