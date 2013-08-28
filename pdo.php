<?php

class PdoDrive {
    public $link;
    public $stmt;
    
    protected $cred;
            
    function __construct($cred) {
        $this->cred = $cred;
        $this->connect();
        return $this;
    }
    
    function connect() {
        try {
            $this->link = new PDO($this->cred['drive'].':host='.$this->cred['host'].';port='.$this->cred['port'].';dbname='.$this->cred['db'], 
                $this->cred['user'], $this->cred['pass'], array( 
                    PDO::ATTR_PERSISTENT => true, 
                    PDO::ATTR_EMULATE_PREPARES => false, 
                    PDO::ATTR_STRINGIFY_FETCHES => false
            ));
            return $this;
        } catch (PDOException $e) {
            print Timer::diff()."Error!: " . $e->getMessage() . "\n";
            die();
        }
    }
    
    public function prepare($sql) {
        $this->stmt = $this->link->prepare($sql);
        return $this;
    }
    
    public function execute() {
        return $this->stmt->execute();
    }
    
    public function query($sql) {
        $this->stmt = $this->link->query($sql);
        if(is_object($this->stmt)) {
            return $this->fetchAssoc();
        } else return false;
    }

    public function exec($sql) {
//        $this->stmt = $this->link->exec($sql);
//        echo $sql;
        return $this->link->query($sql);
//        if(is_object($this->stmt)) {
//            return $this->fetchAssoc();
//        } else return false;
    }

    public function fetchAssoc() {
//        for ($set = array (); $rs = $this->stmt->fetch(PDO::FETCH_ASSOC); $set[] = $rs);
//        for ($set = array (); $rs = $this->stmt->fetch(PDO::FETCH_ASSOC); $set[] = $rs);
        return $this->stmt->fetchAll();
    }
    
    public function fetch() {
        return $this->stmt->fetch(PDO::FETCH_NUM);
    }
    
    public function bindColumn($id, $val) {
        $this->stmt->bindColumn($id, $val); 
        return $this;
    }
    
    public function getError() {
        return $this->link->errorInfo();
    }
    
    public function bindParam($type, $val) {
        $this->stmt->bindParam($type, $val); 
        return $this;
    }
    
    public function bindValue($id, $val, $type = PDO::PARAM_STR) {
        $this->stmt->bindValue($id, $val, $type);
        return $this;
    }
}