<%@taglib uri="http://java.sun.com/jsf/core" prefix="f"%>
<%@taglib uri="http://java.sun.com/jsf/html" prefix="h"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<f:view>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<title>Welcome to Superstore Sales Forecasting</title>
</head>
<body bgcolor="white">
<center><H3>Welcome to Superstore Sales Analysis Prototype!</H3>
<h2>Select what kind of analysis you would like to do.</h2>
<h:form>
<h3>Analysis based on Province/Time. Leave First as '--None--' for a purely time-based analysis.</h3>
<br>
<br>
<br>
<table bordercolor="black">
<tr>
<td colspan="12">
</td>
</tr>
<tr>
<td>
<b>First Input</b>
</td>
<td>
<b>Second Input</b>
</td>
</tr>
<tr>
<td>
<h:selectOneMenu id="one" value="#{userOption.userOption}">
	<f:selectItem itemValue="1" itemLabel="--None--" />
   	<f:selectItem itemValue="2" itemLabel="Province-based-GROSS" />
   	<f:selectItem itemValue="3" itemLabel="Province-based-FORECAST" />
   	<f:selectItem itemValue="4" itemLabel="Product Category-based-GROSS" />
   	<f:selectItem itemValue="5" itemLabel="Product Category-based-FORECAST" />
</h:selectOneMenu>
</td>
<td>
<h:selectOneMenu id="two" value="#{userOption.userSubOption}">
    <f:selectItem itemValue="6" itemLabel="Quarterly" />
   	<f:selectItem itemValue="7" itemLabel="Monthly" />
   	<f:selectItem itemValue="8" itemLabel="Yearly" />
</h:selectOneMenu>
</td>
</tr>
<tr>
<td align="right">
<h:commandButton value="Store inputs" type="submit" action="#{userOption.setOptionCombination}"></h:commandButton>
</td>
</tr>
</table>
</h:form>
<br>
<br>
<br>
<h:form>
<h3>OR, find Top Grossers for your loyalty program</h3>
<table bordercolor="black">
<tr>
<td>
<h:selectOneRadio id="three" value="#{userOption.userOption}">
<f:selectItem itemValue="9" itemLabel="Top Product in each sub category" />
<f:selectItem itemValue="10" itemLabel="Customer loyalty offers" />
</h:selectOneRadio>
</td>
</tr>
<tr>
<td align="center">
<h:commandButton value="Customer Loyalty Program Reports" action="#{userOption.setOptionCombination}"></h:commandButton>
</td>
</tr>
</table>
</h:form>
<br>
<br>
<h:form>
<h:commandLink value="Click here to display results" action="#{userOption.displayResult}"></h:commandLink>
</h:form>
</center>

</body>
</html>
</f:view>