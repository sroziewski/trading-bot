<?php

session_start();

function validate($data){

   $data = trim($data);

   $data = stripslashes($data);

   $data = htmlspecialchars($data);

   return $data;

}

$uname = validate($_POST['uname']);

$pass = validate($_POST['password']);

 if ("b30bd351371c686298d32281b337e8e9" === md5($uname) && "a68d43acea73feb7065e2cee36916e5e" === md5($pass)) {

    echo "Logged in!";

    $_SESSION['user_name'] = $uname;

    header("Location: home.php");

    exit();

 }else {

    header("Location: index.php");

    exit();

}

?>