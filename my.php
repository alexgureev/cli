<?php

class My {

    protected $cred = array(
        'drive' => 'mysql',
        'host' => '192.168.1.166',
        'port' => 3306,
        'db' => 'uinvest_20_08_2013',
        'user' => 'remote',
        'pass' => 'ShPdsfF822yo1ReyI7mt');
    protected $col;
    protected $options;
    const DEBUG = 1;
    const NOTICE = 2;
    const ERROR = 3;
    protected $logLevel;
    protected $row=1;
    protected $db;
    protected $pg;
    protected $tables = array();
    protected $currentTable;
    protected $currentField;
    protected $currentNewField;
    protected $currentType;
    protected $currentNewType;
    protected $allowNull;
    protected $default;
    protected $autoIncrement;
    
    protected $totalRows = 0;
    protected $rowsPerTime = 10;
    protected $currentPage = 0;
    protected $pages = 0;
    protected $page = 1;
    
    protected $sql;
    protected $sqlArray = array();
    protected $typesSql = array();
    protected $seqSql = array();
    protected $indexSql = array();
    protected $output;
    

    function __construct($options) {
        $this->db = new PdoDrive($this->cred);
        $this->pg = new Pg($this->cred);
        $this->col = new Colors();
        $this->options = $options;
        if(isset($options['default'])) {
            $this->options['skip_mask'] .= '_tmp tmp_ vw_ _fix _copy jos_financialpartners_2013_05_29 jos_comprofiler_201304041615 jos_acymailing_userstats_31_05_2013';
        }
        
        if(isset($options['log_level'])) {
            $this->logLevel = $options['log_level'];
        } else {
            $this->logLevel = $this::ERROR;
        }
        
        if(isset($options['debug'])) {
            $this->logLevel = $this::DEBUG;
        } 
        
        return $this;
    }

    public function db() {
        return $this->db;
    }

    public function run() {
        if (isset($this->options['scheme_only']) or isset($this->options['full_dump'])) {
            $this->getTablesList();
            
            if(isset($this->options['database_nuke'])) {
                $this->databaseNuke();
            }
            
            $this->pg->connect2();

            $this->generateTypes();
            $this->createSchema();
        } 

        if (isset($this->options['data_only']) or isset($this->options['full_dump'])) {
            $this->transferPrepare();
        } 
    }

    protected function getTablesList() {
        $this->db->prepare('SHOW TABLES;')->execute();
        $res = $this->db->fetchAssoc();
        file_put_contents(__DIR__ . "/data/tables.txt", "");
        file_put_contents(__DIR__ . "/data/tablesSql.txt", "");

        foreach ($res as $table) {

            $this->currentTable = $table['Tables_in_' . $this->cred['db']];
            if (!$this->isSkipped()) {
                $this->tables[] = $this->currentTable;
                if($this->logLevel<=$this::DEBUG) {
                    $this->sql = 'CREATE TABLE '.$this->currentTable . " (\n";
                    echo Timer::diff() . $this->col->getColoredString('CREATE TABLE '.$this->currentTable . " (\n", "yellow");
                } elseif($this->logLevel<=$this::NOTICE) {
                    echo Timer::diff() . $this->col->getColoredString($this->currentTable, "yellow").$this->col->getColoredString(" - done\n", "cyan");
                }
                
                $this->getTableFields();
                $this->sqlArray[$this->currentTable] = $this->sql;
                file_put_contents(__DIR__ . "/data/tables.txt", $this->currentTable . "\n", FILE_APPEND);
                file_put_contents(__DIR__ . "/data/tablesSql.txt", $this->sql, FILE_APPEND);
            }
        }
    }

    protected function getTableFields() {
        $this->db->prepare("SHOW COLUMNS FROM {$this->currentTable} FROM {$this->cred['db']};")->execute();
        $res =  $this->db->fetchAssoc();
        $this->validateField($res);
    }

    protected function validateField($res) {
        $array = array();
        $this->currentNewField = null;
        $this->currentNewType = null;
        $row = 0;
        $size = sizeof($res)-1;
        
        foreach ($res as $field) {
            $this->currentField = $field['Field'];
            $this->currentType = $field['Type'];
            
            $array[$field['Field']] = $field['Type'] .      // +
                                ":" . $field['Null'] .      // +
                                ":" . $field['Key'] .       // +
                                ":" . $field['Default'] .   //
                                ":" . $field['Extra'];      //
            
            $this->currentNewType = $this->getPgType($field['Type']);
            $this->allowNull = $this->getPgNull($field['Null']);
            $this->key = $this->getPgKey($field['Key']);
            $this->default = $this->getPgDefault($field['Default']);
            $this->autoIncrement = $this->getPgAutoIncrement($field['Extra']);
            
            $this->sql .= '"'.$field['Field'].'" ';
            
            if($field['Type'] != $this->currentNewType) {
                $this->sql .= $this->currentNewType . $this->allowNull . $this->default ;
            } else {
                $this->sql .= $this->currentType . $this->allowNull . $this->default;
            }

            if($row<$size) {
                $this->sql .= ",\n";
            } else {
                $this->sql .= "\n";    
            }
                
            if($this->logLevel==$this::NOTICE) {
//                echo Timer::diff() . "\t" .    $this->col->getColoredString($field['Extra']."\n", "cyan");
            } elseif($this->logLevel<=$this::DEBUG) {
                echo Timer::diff() . "\t" .    $this->col->getColoredString($field['Field'].' ', "cyan");
                if($field['Type'] != $this->currentNewType) {
                    echo $this->col->getColoredString($this->currentNewType, "light_cyan");
                } else {
                    echo $this->col->getColoredString($this->currentType, "light_cyan");
                }

                echo $this->col->getColoredString($this->allowNull, "cyan") . $this->col->getColoredString($this->default, "light_blue");

                if($row<$size) {
                    echo $this->col->getColoredString(",", "light_blue");;
                }               
                
                echo $this->col->getColoredString(" -- ".$array[$field['Field']], "light_gray");
                echo $this->col->getColoredString("\n", "light_blue");
            }
            
            $row++;
        }
        
        $this->sql .= ");\n";
         
        if($this->logLevel<=$this::DEBUG) {
            echo Timer::diff() . $this->col->getColoredString(");\n", "yellow");
        }
    }
    
    protected function getPgType($string) {
        if(strpos($string, 'enum') !== false) {
            return $this->createEnumType($string);
        } elseif(strpos($string, 'tinyint') !== false || strpos($string, 'smallint') !== false) {
            return $this->createSmallIntType();
        } elseif(strpos($string, 'mediumint') !== false) {
            return $this->createIntegerType();
        } elseif(strpos($string, 'int') !== false && strpos($string, 'smallint') === false) {
            return $this->createIntegerType();
        } elseif((strpos($string, 'longtext') !== false) OR 
                (strpos($string, 'tinytext') !== false) OR 
                (strpos($string, 'mediumtext') !== false)) {
            return $this->createTextType($string);
        } elseif((strpos($string, 'decimal') !== false) OR 
                (strpos($string, 'float') !== false) OR 
                (strpos($string, 'double') !== false)) {
            return $this->createDecimalType($string);
        } elseif(strpos($string, 'datetime') !== false) {
            return $this->createDateType($string);
        } elseif(strpos($string, 'binary') !== false) {
            return $this->createBinaryType($string);
        }
        
        return $string;
    }
    
    protected function getPgAutoIncrement($inc) {
        if($inc == 'auto_increment') {
            $this->currentNewType = 'serial';
        }
        
        //TODO on update CURRENT_TIMESTAMP
    }
    
    protected function getPgKey($key) {
        if($key == "PRI") {
            // TODO могут быть составными
            $this->indexSql[] = 'ALTER TABLE "'.$this->currentTable.'" ADD CONSTRAINT "'.$this->currentTable.'_pkey" PRIMARY KEY ("'.$this->currentField.'");';
        } elseif ($key == "UNI") {
            $this->indexSql[] = "CREATE UNIQUE INDEX {$this->currentField}_idx ON {$this->currentTable} ({$this->currentField});";
        } elseif ($key == "MUL") {
            $this->indexSql[] = "CREATE INDEX {$this->currentField}_idx ON {$this->currentTable} ({$this->currentField});";
        }
        
        return $key;
    }
    
    protected function getPgNull($allow) {
        if($allow == "NO") {
            if(in_array($this->currentNewType, array('date', 'time', 'timestamp'))) {
                return ' NULL'; 
            } else {
                return ' NOT NULL'; 
            }
        } elseif($allow == "YES") {
            return ' NULL';
        } else {
            return $this->col->getColoredString(' ERROR', "red") ;
        }
    }
    
    protected function getPgDefault($default) {
        // TODO CURRENT_TIMESTAMP не работает
        if($default == "0000-00-00 00:00:00" || $default == "CURRENT_TIMESTAMP" || $default == "0000-00-00") {
            return ' DEFAULT NULL';
        } else {
            if(strlen($default) == 0) {
                return '';
            } else {
                // TODO string must be in quotes
                if( $default == "b'1'")
                    return ' DEFAULT '.$default;
                else 
                    return " DEFAULT '".$default."'";
            }
        }
    }
    
    protected function createDateType($date) {
        return str_replace('datetime', 'timestamp', $date);
    }
    
    protected function createTextType($string) {
        return str_replace(array('longtext', 'tinytext', 'mediumtext'), 'text', $string);
    }
    
    protected function createSmallIntType() {
        return 'smallint';
    }
    
    protected function createDecimalType($string) {
        if(strpos($this->currentField, 'gold') !== false) {
            return 'decimal(20,5)';
        } elseif(strpos($string, 'decimal') === false ) {
            return 'money';
        } else {
            return str_replace(' unsigned', '', $string);
        }
    }
    
    protected function createIntegerType() {
        return 'integer';
    }
    
    protected function createBinaryType() {
        return 'bytea';
    }
    
    protected function createEnumType($string) {
        $this->typesSql[] = str_replace('enum', "CREATE TYPE {$this->currentTable}_{$this->currentField} AS ENUM ", $string);
        return $this->currentTable . '_' . $this->currentField;
    }    

    protected function isSkipped() {
        $skippedArray = explode("\n", @file_get_contents(__DIR__ . "/data/tablesToSkip.txt"));
        if (in_array($this->currentTable, $skippedArray)) {
            if($this->logLevel<=$this::NOTICE)
                echo Timer::diff() . $this->col->getColoredString($this->currentTable, "yellow") . $this->col->getColoredString(" skipped (in tablesToSkip.txt)\n", "red");
            return true;
        } else {
            $skipMasks = explode(' ', $this->options['skip_mask']);
            if(sizeof($skipMasks)>=1 and isset($this->options['skip_mask']))
            foreach ($skipMasks as $mask) {
                if (strpos($this->currentTable, $mask) !== false) {
                    if($this->logLevel<=$this::NOTICE)
                        echo Timer::diff() . $this->col->getColoredString($this->currentTable, "yellow") . $this->col->getColoredString(" skipped by mask\n", "red");
                    return true;
                }
            }

            return false;
        }
    }
    
    protected function databaseNuke() {
        $result = $this->pg->db()->query('DROP DATABASE IF EXISTS '.$this->pg->getDb2Name());
        if($result !== false ) {
            echo Timer::diff() . $this->col->getColoredString("DATABASE DROPPED\n", "red");
        } else {
            $error = $this->pg->db()->getError();
            echo Timer::diff() . $this->col->getColoredString("{$error[2]}\n", "red");
        }

        $result = $this->pg->db()->query('CREATE DATABASE '.$this->pg->getDb2Name().' ENCODING \'utf8\' TEMPLATE template0;');
        if($result !== false ) {
            echo Timer::diff() . $this->col->getColoredString("DATABASE CREATED\n", "green");
        } else {
            $error = $this->pg->db()->getError();
            echo Timer::diff() . $this->col->getColoredString("{$error[2]}\n", "red");
        }
    }
    
    protected function generateTypes() {
        foreach ($this->typesSql as $type) {
            $result = $this->pg->db()->query($type);
            if($result !== false ) {
                echo Timer::diff() . $this->col->getColoredString("$type\n", "cyan");
            } else {
                $error = $this->pg->db()->getError();
                echo Timer::diff() . $this->col->getColoredString("{$error[2]}\n", "red");
            }
        }
    }
    
    protected function createSchema() {
        if(isset($this->options['scheme_wipe']) || isset($this->options['database_nuke'])) {
            $rtfm = mt_rand(1, 9);
            echo "Are you sure you want to drop tables in Postgre?  Type '$rtfm' to continue: ";
            $handle = fopen("php://stdin", "r");
            $line = fgets($handle);
            if (trim($line) != $rtfm) {
                echo "ABORTING!\n";
                exit;
            }
        }
            
        foreach ($this->sqlArray as $table => $sql) {
            if(isset($this->options['scheme_wipe']) || isset($this->options['database_nuke'])) {
                echo Timer::diff() . $this->col->getColoredString('DROP TABLE ' . $table . "\n", "red");
                $this->pg->db()->query('DROP TABLE IF EXISTS '.$table);
            }

            $result = $this->pg->db()->query($sql);
            if($result === false ) {
                echo Timer::diff() . $this->col->getColoredString("CREATE TABLE $table", "cyan") . $this->col->getColoredString("\t false \n", "red");
                $error = $this->pg->db()->getError();
                echo Timer::diff() . $this->col->getColoredString("{$error[2]}\n", "red");
            }
        }
    }
    
    protected function transferPrepare() {
        if(isset($this->options['data_wipe'])) {
            $rtfm = mt_rand(1, 9);
            echo "Are you sure you want to wipe data in Postgre?  Type '$rtfm' to continue: ";
            $handle = fopen("php://stdin", "r");
            $line = fgets($handle);
            if (trim($line) != $rtfm) {
                echo "ABORTING!\n";
                exit;
            }
        }

        foreach ($this->sqlArray as $table => $sql) {
            $this->page = 1;
            $this->currentTable = $table;
            if(isset($this->options['data_wipe'])) {
                echo Timer::diff() . $this->col->getColoredString('TRUNCATE TABLE '.$table . "\n", "red");
                $this->pg->db()->query('TRUNCATE TABLE '.$table);
            }

            $result = $this->db->query("SELECT COUNT(1) as countFields FROM `$table`");
            if($result!==false) {
                $this->totalRows = $result[0]['countFields'];
                $this->pages = ceil( $this->totalRows / $this->rowsPerTime );
                
                echo Timer::diff() . $this->col->getColoredString("$table", "yellow") . $this->col->getColoredString(" Records: {$result[0]['countFields']}". "\n", "cyan");
                $this->dataTransfer();
            } elseif ($result===false)
                echo Timer::diff() . $this->col->getColoredString("Skip table, records counter return false.". "\n", "light_blue");

//                var_dump($result);
//                $result = $this->pg->db()->query($sql);
        }
    }
    
    protected function dataTransfer() {
        $percentage = round( $this->page / $this->pages * 100, 2 );
        $start = $this->rowsPerTime * ($this->page - 1);
        $this->db->prepare("SELECT * FROM {$this->currentTable} LIMIT 10, 0");
//        $this->db->bindParam(':start', $start, PDO::PARAM_INT);
//        $this->db->bindParam(':end', $this->rowsPerTime, PDO::PARAM_INT);
         
        $var = $this->db->execute();
        while ($row = $this->db->stmt->fetch()) {
            print_r($row);
          } exit;
        $this->page++;
    }
}