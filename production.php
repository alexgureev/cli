#!/usr/bin/php -q
<?php
//set_time_limit(300);
error_reporting(E_ALL ^ E_NOTICE);

//print_r($_SERVER['argv']);
//print_r($_SERVER);

// php script.php -f "value for f" -v -a --required value --optional="optional value" --option выведет:
/*
$shortopts  = "";
$shortopts .= "f:";  // Обязательное значение
$shortopts .= "v::"; // Необязательное значение
$shortopts .= "abc"; // Эти параметры не принимают никаких значений

$longopts  = array(
    "required:",     // Обязательное значение
    "optional::",    // Необязательное значение
    "option",        // Нет значения
    "opt",           // Нет значения
);*/
$shortopts = "";
$shortopts .= "s::";
$shortopts .= "m::";

$longopts  = array(
    "skip_table::",
    "skip_mask::",
    "default::",
    "full_dump::",
    "database_nuke::",
    "scheme_only::",
    "scheme_wipe::",
    "data_only::",
    "data_wipe::",
    "data_skip::",
    "index_only::",
    "select_table::",
    "static_config::",
    "soft::",
    "log_level::"
);
$options = getopt($shortopts, $longopts);

//print_r($options);
//
//echo "\n";

//function replaceOut($str)
//{
//    $numNewLines = substr_count($str, "\n");
//    echo chr(27) . "[0G"; // Set cursor to first column
//    echo $str;
//    echo chr(27) . "[" . $numNewLines ."A"; // Set cursor up x lines
//}
//
//while (true) {
//    replaceOut("First Ln\nTime: " . time() . "\nThird Ln");
//    sleep(1);
//}


include (__DIR__.'/colors.php');
include (__DIR__.'/timer.php');
Timer::init($options);

if (defined('STDIN'))
    echo(Timer::diff()."Running from CLI\n");
else {
    die(Timer::diff()."Not Running from CLI\n");
}

include (__DIR__.'/my.php');
include (__DIR__.'/pg.php');
include (__DIR__.'/pdo.php');

$my = new My($options);
$my->run();

//echo "1Done\r";
//sleep(1);
//echo "2Done\r";
//sleep(1);
//echo "3Done\r";
//sleep(1);
//echo "Are you sure you want to do this?  Type 'yes' to continue: ";
//$handle = fopen("php://stdin", "r");
//$line = fgets($handle);
//if (trim($line) != 'yes') {
//    echo "ABORTING!\n";
//    exit;
//}
//echo "\n";
//echo "Thank you, continuing...\n";

