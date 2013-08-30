<?php

class Pg {

    protected $cred = array(
                'drive'=> 'pgsql',
                'host' => '192.168.1.166', 
                'port' => 5432,
                'db'   => 'switch',
                'db2'   => 'migrate',
                'user' => 'postgres',
                'pass' => 'rwBXOdiTDir5bEVYS8x6' );
            
    protected $db;
    
    function __construct($options) {
        if(isset($options['static_config'])) {
            $static = json_decode(file_get_contents(__DIR__ . "/data/staticConfig{$options['static_config']}.txt"), 1);
            foreach ($static['pg'] as $key => $val) {
                $this->cred[$key] = $val;
            }
        }

        $this->db = new PdoDrive($this->cred);
        return $this;
    }
    
    public function connect2() {
        $this->cred['db'] = $this->cred['db2'];
        $this->db = new PdoDrive($this->cred);
    }
    
    public function db() {
        return $this->db;       
    }
    
    public function getDbName() {
        return $this->cred['db'];
    }
    
    public function getDb2Name() {
        return $this->cred['db2'];
    }
}