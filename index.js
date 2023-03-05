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

    const sumOfoPopulationNode = async () => {
        const data = await db[DATABASE_SCHEMA].api_data.find({});
        const arrayOfDesired = ["2020", "2019", "2018"]
        const desiredData = data.filter(obj => arrayOfDesired.includes(obj.doc_record.Year))
        const result = desiredData.reduce((acc, curr) => acc += curr.doc_record.Population, 0);
        console.log("soma", result);
    }

    const sumOfPopulationInline = async () => {
        const sqlSum = `SELECT SUM((doc_record ->> 'Population')::int) AS sum FROM ${DATABASE_SCHEMA}.api_data WHERE doc_record->> 'Year' IN ('2020', '2019', '2018');`
        const teste = await db.query(sqlSum)
        console.log('result view', teste);
    }
    try {
        await migrationUp();
        // 1. Consumir a API (https://datausa.io/api/data?drilldowns=Nation&measures=Population) e gravar o resultado na tabela "api_data" no na coluna "doc_record".
        await fetchData()
        // ------------------------------------------------------------------------------------------------------------------------------------------------------------
        
    } catch (e) {
        console.log(e.message)
    } finally {
        //         2. Realizar a somatoria da propriedade "Population" dos anos 2020, 2019 e 2018 e appresentar o resultado no console.
        // Implementar de duas formas o algoritmo:

        //     a. em memoria no nodejs usando map, filter, for etc
        await sumOfoPopulationNode();
        // ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
        // b. usando SELECT no postgres, pode fazer um SELECT inline no nodejs.
       await sumOfPopulationInline();
        // -----------------------------------------------------------------------------------------------------------------------------------------------------------------------    
        console.log('count ', await db[DATABASE_SCHEMA].api_data.count());
        console.log('finally');
    }
    console.log('main.js: after start');
})();