/*
 * Copyright 2021 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.graphql.dgs.webflux.autoconfiguration

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.netflix.graphql.dgs.DgsQueryExecutor
import com.netflix.graphql.dgs.DgsReactiveQueryExecutor
import graphql.ExecutionResult
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.server.RequestPredicates.accept
import org.springframework.web.reactive.function.server.RouterFunction
import org.springframework.web.reactive.function.server.RouterFunctions
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers

@Configuration
open class DgsWebFluxAutoConfiguration {

    @Bean
    open fun dgsGraphQlRouter(dgsQueryExecutor: DgsReactiveQueryExecutor): RouterFunction<ServerResponse> {
        val graphQlHandler = GraphQlHandler(dgsQueryExecutor)

        return RouterFunctions.route()
            .POST(
                "/graphql", accept(MediaType.APPLICATION_JSON, MediaType.valueOf("application/graphql")),
                graphQlHandler::graphql
            ).build()
    }

    class GraphQlHandler(private val dgsQueryExecutor: DgsReactiveQueryExecutor) {
        val logger: Logger = LoggerFactory.getLogger(GraphQlHandler::class.java)
        val mapper = jacksonObjectMapper()

        fun graphql(request: ServerRequest): Mono<ServerResponse> {
            val executionResult: Mono<ExecutionResult> =
                request.bodyToMono(String::class.java).map { mapper.readValue<Map<String, Any>>(it) }
                    .flatMap { inputQuery ->

                        val queryVariables: Map<String, Any> = if (inputQuery.get("variables") != null) {
                            @Suppress("UNCHECKED_CAST")
                            inputQuery["variables"] as Map<String, String>
                        } else {
                            emptyMap()
                        }

                        val extensions: Map<String, Any> = if (inputQuery.get("extensions") != null) {
                            @Suppress("UNCHECKED_CAST")
                            inputQuery["extensions"] as Map<String, Any>
                        } else {
                            emptyMap()
                        }

                        logger.debug("Parsed variables: {}", queryVariables)

                        dgsQueryExecutor.execute(
                            inputQuery["query"] as String,
                            queryVariables,
                            extensions,
                            request.headers().asHttpHeaders()
                        )
                    }.subscribeOn(Schedulers.parallel())

            return executionResult.flatMap { result ->
                val graphQlOutput = result.toSpecification()
                ServerResponse.ok().bodyValue(graphQlOutput)
            }
        }
    }
}
