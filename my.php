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
    protected $currentIncrement = array();
    protected $currentType;
    protected $currentNewType;
    protected $currentData;
    protected $dataSkip = 0;
    protected $currentFixedData;
    protected $tableFieldType = array();
    protected $tableFixValue = array();
    protected $allowNull;
    protected $default;
    protected $autoIncrement;
    protected $pgPrepared = 0;
    protected $totalRows = 0;
    protected $rowsPerTime = 100;
    protected $currentPage = 0;
    protected $pages = 0;
    protected $page = 1;
    protected $totalReadTime = 0;
    protected $totalWriteTime = 0;
    protected $debug;
    protected $selectedData = array();
    
    protected $sql;
    protected $sqlArray = array();
    protected $typesSql = array();
    protected $seqSql = array();
    protected $indexSql = array();
    protected $output;
    

    function __construct($options) {
        $this->db = new PdoDrive($this->cred);
        $this->pg = new Pg($options);
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
        
        if(isset($options['data_skip'])) {
            $this->dataSkip = $options['data_skip'];
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
        $this->loadTableFieldsType();
        $this->getTablesList();
        
        if (isset($this->options['scheme_only']) or isset($this->options['index_only']) or isset($this->options['full_dump'])) {
            
            if(isset($this->options['database_nuke'])) {
                $this->databaseNuke();
            }
            
            $this->loadTableFieldsType();
            $this->pg->connect2();

            $this->generateTypes();
            if(!isset($this->options['index_only'])) {
                $this->createSchema();
            }
        } 
        $this->pg->connect2();

        if (isset($this->options['data_only']) or isset($this->options['full_dump'])) {
            $this->loadSelectedData();
            $this->loadTableFixValue();
            $this->transferPrepare();
            $this->createSequences();
        } 
        
        if (isset($this->options['index_only']) or isset($this->options['full_dump'])) {
            $this->createIndexes();
        }
    }
    
    protected function loadTableFieldsType() {
        $content = @file_get_contents(__DIR__ . '/data/tableFieldType.txt');
        $array = explode("\n", $content);
        foreach ($array as $item) {
            $arr = explode(':', $item);
            if(sizeof($arr)>1) {
                $this->tableFieldType[$arr[0]][$arr[1]] = $arr[2];
            }
        }
    }
    
    protected function loadTableFixValue() {
        $content = @file_get_contents(__DIR__ . '/data/tableFixValue.txt');
        $array = explode("\n", $content);
        foreach ($array as $item) {
            $arr = explode(':', $item);
            if(sizeof($arr)>1) {
                $this->tableFixValue[$arr[0]][$arr[1]] = $arr[2];
            }
        }
    }
    
    protected function loadSelectedData() {
        $content = @file_get_contents(__DIR__ . '/data/selectedData.txt');
        $array = explode("\n", $content);
        foreach ($array as $item) {
            $arr = explode('@#:', $item);
            if(sizeof($arr)>1) {
                $this->selectedData[$arr[0]] = json_decode($arr[1], 1);
            }
        }
    }
    
    protected function createIndexes() {
        foreach ($this->indexSql as $sql) {
            Timer::reset();
            $result = $this->pg->db()->exec($sql);
            if($result !== false) {
                echo Timer::diff() . Timer::interval() . $this->col->getColoredString("Sql executed\n", "brown");
            } else {
                echo Timer::diff() . Timer::interval() . $this->col->getColoredString("Sql failed", "red");
                print_r($this->pg->db()->getError());
            }
        }
    }
    
    protected function createSequences() {
        foreach ($this->seqSql as $sql) {
            Timer::reset();
            $result = $this->pg->db()->exec($sql);
            if($result !== false) {
                echo Timer::diff() . Timer::interval() . $this->col->getColoredString("Sql executed\n", "brown");
            } else {
                echo Timer::diff() . Timer::interval() . $this->col->getColoredString("Sql failed", "red");
                print_r($this->pg->db()->getError());
            }
        }
    }

    protected function getTablesList() {
        if(isset($this->options['select_table'])) {
            $res = $this->getSelectedTables();
        } else {
            $this->db->prepare('SHOW TABLES;')->execute();
            $res = $this->db->fetchAssoc();
            file_put_contents(__DIR__ . "/data/tables.txt", "");
            file_put_contents(__DIR__ . "/data/tablesSql.txt", "");
        }
        
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
    
    protected function getSelectedTables() {
        $array = explode(' ', $this->options['select_table']);
        $result = array();
        
        foreach ($array as $table) {
            $result[] = array('Tables_in_' . $this->cred['db'] => $table);
        }
        
        return $result;
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
            
            if(isset($this->tableFieldType[$this->currentTable][$field['Field']])) {
                $this->sql .= $this->tableFieldType[$this->currentTable][$field['Field']];
            } elseif($field['Type'] != $this->currentNewType) {
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
        } elseif(strpos($string, 'bigint') !== false) {
            return $this->createBigIntType();
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
            $this->currentIncrement[$this->currentTable] = $this->currentField;
        }
        
        //TODO on update CURRENT_TIMESTAMP
    }
    
    protected function getPgKey($key) {
        if($key == "PRI") {
            // TODO могут быть составными
            $this->indexSql[] = 'ALTER TABLE "'.$this->currentTable.'" ADD CONSTRAINT "'.$this->currentTable.'_pkey" PRIMARY KEY ("'.$this->currentField.'");';
        } elseif ($key == "UNI") {
            $this->indexSql[] = "CREATE UNIQUE INDEX {$this->currentTable}_{$this->currentField}_uniq_idx ON {$this->currentTable} ({$this->currentField});";
        } elseif ($key == "MUL") {
            $this->indexSql[] = "CREATE INDEX {$this->currentTable}_{$this->currentField}_idx ON {$this->currentTable} ({$this->currentField});";
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
                // TODO convert bits to int
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
        if($string == 'tinytext') {
            return 'varchar(255)';
        } else {
            return str_replace(array('longtext', 'tinytext', 'mediumtext'), 'text', $string);
        }
    }
    
    protected function createSmallIntType() {
        return 'smallint';
    }
    
    protected function createBigIntType() {
        return 'bigint';
    }
    
    protected function createDecimalType($string) {
        if(strpos($this->currentField, 'gold') !== false) {
            return 'decimal(20,5)';
        } elseif(strpos($string, 'decimal') === false ) {
            return 'decimal(16,2)';
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
            if(isset($this->options['skip_table'])) {
                $skip = explode(' ', $this->options['skip_table']);
                $skipMasks = array_merge($skipMasks, $skip);
            }
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
            $this->pgPrepared = 0;
            $this->totalReadTime = $this->totalWriteTime = 0;
    
            $this->currentTable = $table;
            if(isset($this->options['data_wipe'])) {
                if(is_array($this->selectedData[$this->currentTable])) {
                    if(isset($this->selectedData[$this->currentTable]['table'])) { $newTable = $this->selectedData[$this->currentTable]['table']; } else { $newTable = $this->currentTable; }
                    echo Timer::diff() . $this->col->getColoredString('TRUNCATE TABLE '.$newTable . "\n", "red");
                    $this->pg->db()->query('TRUNCATE TABLE '.$newTable);
                } else {
                    echo Timer::diff() . $this->col->getColoredString('TRUNCATE TABLE '.$this->currentTable . "\n", "red");
                    $this->pg->db()->query('TRUNCATE TABLE '.$this->currentTable);
                }
            }

            $result = $this->db->query("SELECT COUNT(1) as countFields FROM `{$this->currentTable}`");
            if($result!==false) {
                $this->totalRows = $result[0]['countFields'];
                $this->pages = ceil( $this->totalRows / $this->rowsPerTime );
                if(isset($this->options['soft'])) {
                    $result = $this->pg->db()->query("SELECT COUNT(1) as countFields FROM {$this->currentTable}");
                    if($this->totalRows == 0) {
                        echo Timer::diff() . $this->col->getColoredString("{$this->currentTable}", "yellow") . $this->col->getColoredString("\t source table is empty\n", "purple");
                        continue;
                    } elseif ($this->totalRows == $result[0]['countfields']) {
                        echo Timer::diff() . $this->col->getColoredString("{$this->currentTable}", "yellow") . $this->col->getColoredString("\t data already inserted, skip by 'soft' param\n", "brown");
                        continue;
                    } elseif ($result[0]['countfields']>0){
//                        var_dump($result);
                        echo Timer::diff() . $this->col->getColoredString("{$this->currentTable}", "yellow") . $this->col->getColoredString("\t table not empty, skipping\n", "cyan");
                        continue;
                    }
                }
                
                $this->prepareSeq();
                
                $this->db->prepare("SELECT * FROM `{$this->currentTable}` LIMIT ?, ?");
                
                echo Timer::diff() . $this->col->getColoredString("{$this->currentTable}\n\n", "yellow");
//                $this->dataTransfer();
            } elseif ($result===false)
                echo Timer::diff() . $this->col->getColoredString("Skip table, records counter return false.". "\n", "light_blue");
        }
    }
    
    protected function prepareSeq() {
        if(isset($this->currentIncrement[$this->currentTable])) {
            $result = $this->db->query("SHOW TABLE STATUS LIKE '{$this->currentTable}'");
            $this->seqSql[] = "ALTER SEQUENCE {$this->currentTable}_{$this->currentIncrement[$this->currentTable]}_seq RESTART WITH {$result[0]['Auto_increment']}";
        }
    }
    
    protected function dataTransfer() {
        while(  $this->page<=$this->pages &&
                ( $this->dataSkip > $this->page * $this->rowsPerTime || $this->dataSkip == 0)) {
            
            
            $percentage = round( $this->page / $this->pages * 100, 2 );
            $start = $this->rowsPerTime * ($this->page - 1);

            $this->db->bindValue(1, $start, PDO::PARAM_INT);
            $this->db->bindValue(2, $this->rowsPerTime, PDO::PARAM_INT);

            $var = $this->db->execute();
            if($var == true) {
                
                $colCount = $this->db->stmt->columnCount();
                
                if($this->pgPrepared == 0) {
                    $this->pgPrepare($colCount);
                }

                while($row = $this->db->stmt->fetch()) {
                    Timer::reset();
                    $this->currentData = array();
                    $param = 1;
                    for($i = 0; $i<=$colCount-1; $i++) {
                        if(is_array($this->selectedData[$this->currentTable])) {
                            if(isset($this->selectedData[$this->currentTable]['values'][$i])) {
                                $newI = $this->selectedData[$this->currentTable]['values'][$i];
//                                print_r($this->selectedData[$this->currentTable]['values']);
                                $val = $this->fixValues($row[$newI], $newI);
                                $this->currentData[$newI] = $row[$newI];
                                $this->currentFixedData[$newI] = $val;

                                if(ctype_digit($val)) {
                                    $this->pg->db()->bindValue(($newI), $val, PDO::PARAM_INT);    
                                } elseif($val === null ) {
                                    $this->pg->db()->bindValue(($newI), $val, PDO::PARAM_NULL); 
                                } else {
                                    $this->pg->db()->bindValue(($newI), iconv(mb_detect_encoding($val), 'UTF-8', $val));
                                }
                                $param++;
                            }
                            else continue;
                        } else {
                            $newI = $i;
                            $val = $this->fixValues($row[$newI], $newI);
                            $this->currentData[$newI] = $row[$newI];
                            $this->currentFixedData[$newI] = $val;

                            if(ctype_digit($val)) {
                                $this->pg->db()->bindValue(($newI+1), $val, PDO::PARAM_INT);    
                            } elseif($val === null ) {
                                $this->pg->db()->bindValue(($newI+1), $val, PDO::PARAM_NULL); 
                            } else {
                                $this->pg->db()->bindValue(($newI+1), iconv(mb_detect_encoding($val), 'UTF-8', $val));
                            }
                        }
                            
                    }
                    $reedTime = Timer::interval();
                    $this->totalReadTime += $reedTime;
                    Timer::reset();
                    $res = $this->pg->db()->execute();
                    $writeTime = Timer::interval();
                    $this->totalWriteTime += $writeTime;
                    
                    if($res == false ) {
                        echo Timer::diff() . "\t ERROR IN POSTGRESQL rows($colCount)\n";
//                        print_r($this->currentData);
                        var_dump($this->currentData);
//                        print_r($this->currentFixedData);
//                        var_dump($row);
                        echo Timer::diff() . $this->col->getColoredString(print_r($this->pg->db()->getError(), 1). "\n", "red");
                        exit;
                    }
                }
                echo $this->col->getColoredString( "\033[1F" . Timer::diff(), "light_green" ) . 
                     $this->col->getColoredString( "Records: ", "cyan") . 
                     $this->col->getColoredString( $this->page * $this->rowsPerTime . "/", "magenta") .
                     $this->col->getColoredString( $this->totalRows . " ", "green" ) . 
                     $this->col->getColoredString( $percentage . "% ", "yellow" ) .
                     $this->col->getColoredString( $this->rowsPerTime . "pt ", "green" ) .
                     $this->col->getColoredString( $reedTime . " ", "magenta" ) . 
                     $this->col->getColoredString( $writeTime."                          \n", "purple" ); 
                     
            } else {
                echo Timer::diff() . "\t ERROR IN MYSQL\n";
                echo Timer::diff() . $this->col->getColoredString(print_r($this->db->getError(), 1). "\n", "red");
                exit;
            }

            $this->page++;
        }
        
        echo "\n";
        
        if($this->page>$this->pages) {
            echo Timer::diff() . $this->col->getColoredString("All data transfered\t", "light_gray");
        } elseif ($this->page!=$this->pages) {
            echo Timer::diff() . $this->col->getColoredString("Skipped by 'data_skip'\t", "light_purple");
        }
        
        echo $this->col->getColoredString( "R:".round($this->totalReadTime, 0) . " ", "magenta" ) . 
             $this->col->getColoredString( "W:".round($this->totalWriteTime, 0) . " \n", "purple" );
    }
    
    protected function pgPrepare($cols) {
        $sql = "INSERT INTO {$this->currentTable} VALUES (";
//        print_r($this->selectedData[$this->currentTable]); exit;
        if(is_array($this->selectedData[$this->currentTable])) {
            $sql .= $this->selectedData[$this->currentTable]['insert'];
        } else {
            for($i = 0; $i<=$cols-1; $i++) {
                $sql .= '?';
                if($i<$cols-1)
                    $sql .= ', ';
            }
        }
        $sql .= ");";
        //"INSERT INTO {$this->currentTable} VALUES (?, ?);"
        $this->pg->db()->prepare($sql);
        $this->pgPrepared = 1;
    }
    
    protected function fixValues($val, $i) {
        if(isset($this->tableFixValue[$this->currentTable][$i]) ) {
            $change = 0;
            try {
                eval($this->tableFixValue[$this->currentTable][$i]);
                if($change == 1) {
                    return $result;
                }
            } catch (Exception $e) {
                // TODO exec issue
            }
        }
        
        if($val == '0000-00-00 00:00:00' || $val == '0000-00-00') {
           return NULL;
        }
        // base_convert($row['my_bit'],16,10)
        return $val;
    }
}