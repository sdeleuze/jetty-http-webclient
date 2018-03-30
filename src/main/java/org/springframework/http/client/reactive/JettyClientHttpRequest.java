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

import java.net.HttpCookie;
import java.net.URI;
import java.util.Collection;

import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.reactive.client.ContentChunk;
import org.eclipse.jetty.reactive.client.ReactiveRequest;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.util.Assert;

/**
 * {@link ClientHttpRequest} implementation for the Jetty HTTP client.
 * TODO Should implement ZeroCopyHttpOutputMessage?
 *
 * @author Sebastien Deleuze
 * @since 5.1
 */
public class JettyClientHttpRequest extends AbstractClientHttpRequest {

	private final Request jettyRequest;

	// TODO Check with Arjen what is relevant here
	private final DataBufferFactory bufferFactory = new DefaultDataBufferFactory();

	private ReactiveRequest reactiveRequest;

	private Mono<ClientHttpResponse> response;

	public JettyClientHttpRequest(Request jettyRequest) {
		this.jettyRequest = jettyRequest;
	}

	@Override
	protected void applyHeaders() {
		getHeaders().entrySet().forEach(e -> e.getValue().forEach(v -> this.jettyRequest.header(e.getKey(), v)));
		if (!getHeaders().containsKey(HttpHeaders.ACCEPT)) {
			this.jettyRequest.header(HttpHeaders.ACCEPT, "*/*");
		}
	}

	@Override
	protected void applyCookies() {
		getCookies().values().stream().flatMap(Collection::stream)
				.map(cookie -> new HttpCookie(cookie.getName(), cookie.getValue()))
				.forEach(this.jettyRequest::cookie);
	}

	@Override
	public HttpMethod getMethod() {
		HttpMethod method = HttpMethod.resolve(this.jettyRequest.getMethod());
		Assert.notNull(method, "Method must not be null");
		return method;
	}

	@Override
	public URI getURI() {
		return this.jettyRequest.getURI();
	}

	@Override
	public DataBufferFactory bufferFactory() {
		return this.bufferFactory;
	}

	@Override
	public Mono<Void> writeWith(Publisher<? extends DataBuffer> publisher) {
		String contentType = jettyRequest.getHeaders().contains(HttpHeader.CONTENT_TYPE) ?
				jettyRequest.getHeaders().getField(HttpHeader.CONTENT_TYPE).getValue():
				MediaType.APPLICATION_OCTET_STREAM_VALUE;
		ReactiveRequest.Content requestContent =
				ReactiveRequest.Content.fromPublisher(Flux.from(publisher).map(DataBuffer::asByteBuffer).map(ContentChunk::new),
						contentType);
		ReactiveRequest reactiveRequest =
				ReactiveRequest.newBuilder(jettyRequest).content(requestContent).build();
		return doCommit(() -> {
			this.response =
					Mono.from(reactiveRequest.response((reactiveResponse, responseContent) ->
							Mono.just(new
									JettyClientHttpResponse(reactiveResponse,
									Flux.from(responseContent).map(chunk -> {
										DataBuffer dataBuffer = bufferFactory.wrap(chunk.buffer);
										chunk.callback.succeeded();
										return dataBuffer;
									})))));
			return Mono.empty();
		});
	}

	@Override
	public Mono<Void> writeAndFlushWith(Publisher<? extends Publisher<? extends DataBuffer>> body) {
		String contentType = this.jettyRequest.getHeaders().getField(HttpHeader.CONTENT_TYPE).getValue();
		ReactiveRequest.Content content = ReactiveRequest.Content.fromPublisher(Flux
				.from(body)
				.flatMap(publisher -> publisher)
				.map(buffer -> new ContentChunk(buffer.asByteBuffer())), contentType);
		this.reactiveRequest = ReactiveRequest.newBuilder(this.jettyRequest).content(content).build();
		return doCommit(this::completes);
	}

	@Override
	public Mono<Void> setComplete() {
		if (this.reactiveRequest == null) {
			this.reactiveRequest = ReactiveRequest.newBuilder(this.jettyRequest).build();
		}
		return doCommit(this::completes);
	}

	private Mono<Void> completes() {
		this.response = Mono.from(
				this.reactiveRequest.response((reactiveResponse, contentChunks) -> {
				Flux<DataBuffer> content = Flux.from(contentChunks).map(chunk -> {
					DataBuffer dataBuffer = this.bufferFactory.wrap(chunk.buffer);
					chunk.callback.succeeded();
					return dataBuffer;
				});
				return Mono.just(new JettyClientHttpResponse(reactiveResponse, content));
			}));
		return Mono.empty();
	}

	public Mono<ClientHttpResponse> getResponse() {
		return this.response;
	}
}
