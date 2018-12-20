<?php
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