// route query
const sql = (params, query) => {

    let postgissql = `
    SELECT DISTINCT ref FROM public.hainan_road_split where (ref like '%G%' or ref like '%S%' or ref like '%X%' or ref like '%Y%' or ref like '%c%')
    `
    return postgissql;
}

// route schema
const schema = {
    description:
        '查询数据量',
    tags: ['service'],
    summary: 'return JSON',

    querystring: {
        filter: {
            type: 'string',
            description: 'Optional filter parameters for a SQL WHERE statement.'
        }
    }
}

// create route
module.exports = function (fastify, opts, next) {
    fastify.route({
        method: 'GET',
        url: '/list_sql',
        schema: schema,
        handler: function (request, reply) {
            fastify.pg.connect(onConnect);

            function onConnect (err, client, release) {
                if (err) {
                    request.log.error(err)
                    return reply.code(500).send({ "error": "Database connection error." })
                }
                client.query(
                    sql(request.params, request.query),
                    function onResult (err, result) {
                        release()
                        reply.send(err || result.rows)
                    }
                )
            }
        }
    })
    next()
}

module.exports.autoPrefix = '/v1'
