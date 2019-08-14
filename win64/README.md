please note that this setup was tested with Visual Studio 2017 and PostgreSQL 11. You may need to change paths to include files to respect correct location of PostgreSQL headers and libs. 

Go to the Solution pane and for each project (pgtt and pgtt_bgw) check paths in

*Project->Properties->C/C++/General/Additional Include Directories*
*Project->Properties->Linker/General/Additional Library Directories*

Also expect .dlls to be generated in the x64 dir after build. These need to go in the lib directory of PostgreSQL and the pgtt.control and pgtt--1.2.0.sql need to go in Postgre's share\extension directory. 