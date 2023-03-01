db = connect( 'mongodb://localhost/seconddb' );

var file = cat('/home/jovyan/__DATA/IBDT_Spring_2023/topic_2/teachers-20230203.json');
var data = JSON.parse(file);

db.teachers.insert(data);

printjson( db.teachers.findOne() );