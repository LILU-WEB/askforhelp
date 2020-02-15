import "reflect-metadata";
import 'dotenv/config';

import { Server, IncomingMessage, ServerResponse } from "http";
import { buildSchema, Resolver, Query} from "type-graphql";

import * as fastify from "fastify";
const GQL = require("fastify-gql");

@Resolver()
class userResolver {
    @Query(returns => String)
    async hello() {
        throw new Error("this is demo");
    }
}

export const setErrorHandler = (async (error: any, req: any, reply: any) => {
    console.log(error);
    reply.send()
})


const main = async () => {

    const app: fastify.FastifyInstance<
        Server,
        IncomingMessage,
        ServerResponse
        > = fastify({});

    const schema = await buildSchema({
        resolvers: [userResolver],
        validate: false
    });

    app.register(GQL, {
        schema,
        graphiql: 'playground',
        jit: 1,
        errorHandler: setErrorHandler
    });

    app.listen(3000, "0.0.0.0", () => {
        console.log(
            "server started at http://localhost:3000/playground.html"
        );
    });
};

main();