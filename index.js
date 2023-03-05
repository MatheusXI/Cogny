const { DATABASE_SCHEMA, DATABASE_URL, SHOW_PG_MONITOR } = require('./config');
const massive = require('massive');
const monitor = require('pg-monitor');
const axios = require('axios');

// Call start
(async () => {
    console.log('main.js: before start');
    console.log('schema',DATABASE_SCHEMA);

    const db = await massive({
        connectionString: DATABASE_URL,
        ssl: { rejectUnauthorized: false },
    }, {
        // Massive Configuration
        scripts: process.cwd() + '/migration',
        allowedSchemas: [DATABASE_SCHEMA],
        whitelist: [`${DATABASE_SCHEMA}.%`],
        excludeFunctions: true,
    }, {
        // Driver Configuration
        noWarnings: true,
        error: function (err, client) {
            console.log(err);
            //process.emit('uncaughtException', err);
            //throw err;
        }
    });

    if (!monitor.isAttached() && SHOW_PG_MONITOR === 'true') {
        monitor.attach(db.driverConfig);
    }

    const execFileSql = async (schema, type) => {
        return new Promise(async resolve => {
            const objects = db['user'][type];

            if (objects) {
                for (const [key, func] of Object.entries(objects)) {
                    console.log(`executing ${schema} ${type} ${key}...`);
                    await func({
                        schema: DATABASE_SCHEMA,
                    });
                }
            }

            resolve();
        });
    };

    //public
    const migrationUp = async () => {
        return new Promise(async resolve => {
            await execFileSql(DATABASE_SCHEMA, 'schema');

            //cria as estruturas necessarias no db (schema)
            await execFileSql(DATABASE_SCHEMA, 'table');
            await execFileSql(DATABASE_SCHEMA, 'view');

            console.log(`reload schemas ...`)
            await db.reload();

            resolve();
        });
    };

    const fetchData = async () => {
        const response = await axios.get("https://datausa.io/api/data?drilldowns=Nation&measures=Population", {
            timeout: 6000,
        })
        await db[DATABASE_SCHEMA].api_data.destroy({})
        const data = response.data.data
        console.log(data, 'teste')
        data.map(obj => {
            db[DATABASE_SCHEMA].api_data.insert({
                doc_record: obj
            })
        })
    }
    try {
        await migrationUp();
        // 1. Consumir a API (https://datausa.io/api/data?drilldowns=Nation&measures=Population) e gravar o resultado na tabela "api_data" no na coluna "doc_record".
        await fetchData()
        // ------------------------------------------------------------------------------------------------------------------------------------------------------------
        
    
        
    } catch (e) {
        console.log(e.message)
    } finally {
        console.log('db', await db[DATABASE_SCHEMA].api_data.find({}));
        console.log('count ', await db[DATABASE_SCHEMA].api_data.count());
        console.log('finally');
    }
    console.log('main.js: after start');
})();