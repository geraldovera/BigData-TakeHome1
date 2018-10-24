# You sql follows
create table estudiantesPR(Region string, Distrito string, IdEscuela int, Escuela string, Nivel string, Sexo string, IdEstudiante int) row format delimited fields terminated by ',';
load data local inpath '/home/geraldo_vera/studentsPR.csv' overwrite into table estudiantesPR;

create table escuelasPR(Region string, Distrito string, Ciudad string, IdEscuela int, Escuela string, Nivel string, NumeroCB int) row format delimited fields terminated by ',';
load data local inpath '/home/geraldo_vera/escuelasPR.csv' overwrite into table escuelasPR;

select escuelasPR.Ciudad, escuelasPR.Nivel, count(*) from escuelasPR group by escuelasPR.Ciudad, escuelasPR.Nivel;

select escuelasPR.Region, escuelasPR.Ciudad, count(*) from escuelasPR, estudiantesPR where escuelasPR.IdEscuela
 = estudiantesPR.IdEscuela group by escuelasPR.Region, escuelasPR.Ciudad;

select escuelasPR.Ciudad, escuelasPR.Nivel, count(*) from escuelasPR, estudiantesPR where
 estudiantesPR.IdEscuela = escuelasPR.IdEscuela and escuelasPR.Ciudad = 'Ponce'
 and estudiantesPR.Nivel = 'SPR' and estudiantePR.Sexo = 'F' group by escuelasPR.Ciudad, escuelasPR.Nivel;

select escuelasPR.Region, escuelasPR.Distrito, escuelasPR.Ciudad, count(*) from estudiantesPR, escuelasPR
 where estudiantesPR.IdEscuela = escuelasPR.IdEscuela and estudiantesPR.Sexo='M'
 group by escuelasPR.Region, escuelasPR.Distrito, escuelasPR.Ciudad;



