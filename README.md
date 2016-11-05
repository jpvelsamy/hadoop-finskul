# hadoop-finskul
Contains tutorial code

Check out the code using git and create an eclipse template using maven and import the project. This code will work only with the bonsai cluster that you need to setup using the hadoop docker projects present as other repositories under my account.

1. Used maven archetype
mvn archetype:generate -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false -DgroupId=finskul -DartifactId=com.finskul.hadoop

2. Added hadoop 2.7.1 dependencies
Refere pom.xml for knowing the dependency list

3. Added eclipse template
mvn eclipse:eclipse


