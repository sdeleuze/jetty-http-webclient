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

import org.eclipse.jetty.reactive.client.ReactiveResponse;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseCookie;
import org.springframework.util.Assert;
import org.springframework.util.MultiValueMap;

/**
 * {@link ClientHttpResponse} implementation for the Jetty HTTP client.
 *
 * @author Sebastien Deleuze
 * @since 5.1
 */
public class JettyClientHttpResponse implements ClientHttpResponse {

	private final ReactiveResponse reactiveResponse;

	private final Flux<DataBuffer> content;

	public JettyClientHttpResponse(ReactiveResponse reactiveResponse, Publisher<DataBuffer> content) {
		Assert.notNull(reactiveResponse, "reactiveResponse should not be null");
		Assert.notNull(content, "content should not be null");
		this.reactiveResponse = reactiveResponse;
		this.content = Flux.from(content);
	}

	@Override
	public HttpStatus getStatusCode() {
		return HttpStatus.valueOf(this.reactiveResponse.getStatus());
	}

	@Override
	public MultiValueMap<String, ResponseCookie> getCookies() {
		return null;
	}

	@Override
	public Flux<DataBuffer> getBody() {
		return this.content;
	}

	@Override
	public HttpHeaders getHeaders() {
		HttpHeaders headers = new HttpHeaders();
		this.reactiveResponse.getHeaders().stream()
				.forEach(e -> headers.add(e.getName(), e.getValue()));
		return headers;
	}
}
