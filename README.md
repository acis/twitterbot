TwitterBot

Installation:

Install nodejs v.0.10 ( http://nodejs.org/ ) and npm, depending on your platform (https://npmjs.org/doc/README.html)
Some helpful information can be found here: https://gist.github.com/isaacs/579814 

Run npm install in the project root directory.

Download neo4j 2.0.0 ( http://www.neo4j.org/download ) and start neo4j server. 
Set up an empty mysql database and run the init.sql script. Default values are localhost for the host with user root and no password.  

Fill out keys.js.template with your twitter account details and mysql connection details ( if needed ) and rename to keys.js.

Run "node twitterbot" in the root directory. 




