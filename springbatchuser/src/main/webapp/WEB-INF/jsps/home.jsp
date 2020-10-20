<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
    <%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
    <!DOCTYPE html>
<html lang="en">
<head>
  <title>Bootstrap Example</title>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css">
  <script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script>
  <script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/js/bootstrap.min.js"></script>
  <style>
    /* Remove the navbar's default margin-bottom and rounded borders */ 
    .navbar {
      margin-bottom: 0;
      border-radius: 0;
    }
    
    /* Set height of the grid so .sidenav can be 100% (adjust as needed) */
    .row.content {height: 450px}
    
    /* Set gray background color and 100% height */
    .sidenav {
      padding-top: 20px;
      background-color: #f1f1f1;
      height: 100%;
    }
    
    /* Set black background color, white text and some padding */
    footer {
      background-color: #555;
      color: white;
      padding: 15px;
    }
    
    /* On small screens, set height to 'auto' for sidenav and grid */
    @media screen and (max-width: 767px) {
      .sidenav {
        height: auto;
        padding: 15px;
      }
      .row.content {height:auto;} 
    }
  </style>
</head>
<body>

<nav class="navbar navbar-inverse">
  <div class="container-fluid">
    <div class="navbar-header">
      <button type="button" class="navbar-toggle" data-toggle="collapse" data-target="#myNavbar">
        <span class="icon-bar"></span>
        <span class="icon-bar"></span>
        <span class="icon-bar"></span>                        
      </button>
     
    </div>
    <div class="collapse navbar-collapse" id="myNavbar">
      <ul class="nav navbar-nav">
        <li class="active"><a href="/home">Home</a></li>
       
      </ul>
      <ul class="nav navbar-nav navbar-right">
        <li><a href="#"><span class="glyphicon glyphicon-log-in"></span> Logout</a></li>
      </ul>
    </div>
  </div>
</nav>
  
<div class="container-fluid text-center">    
  <div class="row content">
    <div class="col-sm-2 sidenav">
     
    </div>
    
    <div class="col-sm-8 text-left"> 
      <h1>Hi Admin Welcome to Batch Processing Site !...</h1>
      <p>Kindly select the type of batch processing to execute files in the folder</p>
      <hr>
      <h3>For Auto Job Scheduling</h3>
      <p><c:if test="${logi ne 'login'}">

<button><a href="/load">Auto Job Scheduling</a></button>

  <hr>upload files

 <form method="post" action="/load/uploadfiles" enctype="multipart/form-data">
 
 <input type="file" name="fileUpload" size="50" />
 
 <input type="submit" value="Upload" />
 </form>


</c:if> <!-- <hr><button><a href="/autohistory">Auto Job Scheduling history</a></button> --></p>

<hr>

<h3>For Manual Scheduling</h3>

<p> <c:if test="${logi ne 'login'}">



 <button> <a href="/load/manualmodelist">Manual Job Scheduling</a></button>
</c:if></p>
    </div>
    <c:if test="${logi eq 'login'}">
<form action="/logicheck" method="post">
<div align="center">
<h3>${logfail}</h3>

UserName : <input type="text" name = "userName" placeholder="Enter username"><br>
Password : <input type="password" name="password"
 placeholder="Enter password">
<hr>
<input type = "submit" value="Login">
</div>

</form>
</c:if>

<%-- <c:if test="${logi ne 'login'}">

<button><a href="/automode">Auto Job Scheduling</a></button>  

 <button> <a href="/manualmodelist">Manual Job Scheduling</a></button>
</c:if> --%>
<br>
${successorfailure} <br>

${cre}


<!-- <br> <a href="/firstautomodetest">first auto test </a> -->



${hello}

<c:if test="${cahis eq 'checkahis' }">

<c:if test="${lisAuto.size() == 0}">

No Auto scheduling histories available

</c:if>
<c:if test="${lisAuto.size() != 0}">
<table border=1>
<h3>Auto Scheduling History</h3>
<tr><th>SNO</th><th>File Name</th><th>Date </th><th>Status</th></tr>
<c:forEach items="${lisAuto}" var = "lh">

<tr><td>${lh.sNo}</td><td>${lh.fileName}</td><td>${lh.dateTime}</td><td>${lh.status}</td></tr>

</c:forEach>
</table>

</c:if>
</c:if>



<c:if test="${chis eq 'checkhis' }">

<c:if test="${lisHist.size() == 0}">

No manual scheduling histories available

</c:if>
<c:if test="${lisHist.size() != 0}">
<table border=1>
<h3>Manual Scheduling History</h3>
<tr><th>SNO</th><th>File Name</th><th>Date </th><th>Status</th></tr>
<c:forEach items="${lisHist}" var = "lh">

<tr><td>${lh.sNo}</td><td>${lh.fileName}</td><td>${lh.dateTime}</td><td>${lh.status}</td></tr>

</c:forEach>
</table>

</c:if>
</c:if>

<c:if test="${manualtest eq 'checkmanual'}">

<button><a href="/manualhistory">Manal scheduling history</a></button>
<form action="/manualmode" method="get">


<table>
<h3>Select files </h3>
<c:forEach items="${fName}" var="fileName"> 
<tr> <td><input type="checkbox" name="fnames" value= "${fileName}"></td><td>
   ${fileName}</td>
</tr>
</c:forEach></table>
calender  <input type="datetime-local"  value="2020-10-08 19:32:00" step="1" name="datetimeloc">
<input type="submit" value="manual search">
</form>
</c:if>
<c:if test="${not empty manualdone}">
			<script>
				alert("Manual batch scheduled");
			</script>
			<c:remove var="manualdone" scope="session" />
		</c:if>



<c:if test="${not empty autodone}">
			<script>
				alert("Auto batch started");
			</script>
			<c:remove var="autodone" scope="session" />
		</c:if>

</div>

<footer class="container-fluid text-center">
  <p>Batch processing site !.</p>
</footer>

</body>
</html>
    
<%-- <!DOCTYPE html>
<html>
<head>
<meta charset="ISO-8859-1">
<title>Insert title here</title>
</head>
<body>

<c:if test="${logi eq 'login'}">
<form action="/logicheck" method="post">
<div align="center">
<h3>${logfail}</h3>

UserName : <input type="text" name = "userName" placeholder="Enter username"><br>
Password : <input type="password" name="password"
 placeholder="Enter password">
<br>
<input type = "submit" value="Login">
</div>

</form>
</c:if>

<c:if test="${logi ne 'login'}">

<button><a href="/automode">Auto Job Scheduling</a></button>  

 <button> <a href="/manualmodelist">Manual Job Scheduling</a></button>
</c:if>
<br>
${successorfailure} <br>

${cre}


<!-- <br> <a href="/firstautomodetest">first auto test </a> -->



${hello}

<c:if test="${chis eq 'checkhis' }">

<c:if test="${lisHist.size() == 0}">

No manual scheduling histories available

</c:if>
<c:if test="${lisHist.size() != 0}">
<table>

<tr><th>SNO</th><th>File Name</th><th>Date </th><th>Status</th></tr>
<c:forEach items="${lisHist}" var = "lh">

<tr><td>${lh.sNo}</td><td>${lh.fileName}</td><td>${lh.dateTime}</td><td>${lh.status}</td></tr>

</c:forEach>
</table>

</c:if>
</c:if>

<c:if test="${manualtest eq 'checkmanual'}">

<button><a href="/manualhistory">Manal scheduling history</a></button>
<form action="/manualmode" method="get">


<table>

<c:forEach items="${fName}" var="fileName"> 
<tr> <td><input type="checkbox" name="fnames" value= "${fileName}"></td><td>
   ${fileName}</td>
</tr>
</c:forEach></table>
calender  <input type="datetime-local"  value="2020-10-08 19:32:00" step="1" name="datetimeloc">
<input type="submit" value="manual search">
</form>
</c:if>
</body>
</html> --%>