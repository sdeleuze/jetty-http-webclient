/*
 * Copyright 2002-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.http.client.reactive;

import java.net.URI;
import java.util.function.Function;

import org.eclipse.jetty.client.HttpClient;
import reactor.core.publisher.Mono;

import org.springframework.http.HttpMethod;

/**
 * Jetty implementation of {@link ClientHttpConnector}.
 *
 * @author Sebastien Deleuze
 * @since 5.1
 */
public class JettyClientHttpConnector implements ClientHttpConnector {

	private final HttpClient httpClient;

	public JettyClientHttpConnector() {
		this.httpClient = new HttpClient();
	}

	@Override
	public Mono<ClientHttpResponse> connect(HttpMethod method, URI uri,
			Function<? super ClientHttpRequest, Mono<Void>> requestCallback) {

		if (!uri.isAbsolute()) {
			return Mono.error(new IllegalArgumentException("URI is not absolute: " + uri));
		}

		if (!httpClient.isStarted()) {
			try {
				this.httpClient.start();
			}
			catch (Exception ex) {
				return Mono.error(ex);
			}
		}
		org.springframework.http.client.reactive.JettyClientHttpRequest clientHttpRequest =
				new JettyClientHttpRequest(httpClient.newRequest(uri).method(method.toString()));
		return requestCallback.apply(clientHttpRequest).then(clientHttpRequest.getResponse());
	}


}
