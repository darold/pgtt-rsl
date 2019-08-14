
Please note that this setup was successfully built using Visual Studio 2017 Enterprise (licensed) and also Visual Studio 2019 Enterprise (trial), against Windows SDK 10.0.18362.0 and Platfrom Toolset v141 and v142 targeting PostgreSQL 11. 

You may need to change paths to include files to respect correct location of PostgreSQL headers and libs. 

Go to the Solution pane and for each project (pgtt and pgtt_bgw) check paths in

*Project->Properties->C/C++/General/Additional Include Directories*
*Project->Properties->Linker/General/Additional Library Directories*

Also expect .dlls to be generated in the x64 dir after build. These need to go in the lib directory of PostgreSQL and the pgtt.control and pgtt--1.2.0.sql need to go in Postgre's share\extension directory. 

Should you need to build for 32-bit postgre - make the changes to the project config in the solution file. 