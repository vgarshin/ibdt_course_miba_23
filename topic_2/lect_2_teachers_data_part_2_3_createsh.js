db = connect( 'mongodb://localhost/thirddb' );

var data = JSON.parse(fs.readFileSync('/home/jovyan/__DATA/IBDT_Spring_2023/topic_2/teachers-20230203.json', 'utf8'));

db.teachers.insert(data);

printjson( db.teachers.findOne() );