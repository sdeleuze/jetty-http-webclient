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

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpCookie;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;

import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.reactive.client.ContentChunk;
import org.eclipse.jetty.reactive.client.ReactiveRequest;
import org.eclipse.jetty.util.Callback;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBuffer;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.core.io.buffer.PooledDataBuffer;
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

	private final DefaultDataBufferFactory bufferFactory = new DefaultDataBufferFactory();

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
		this.reactiveRequest = ReactiveRequest.newBuilder(jettyRequest).content(requestContent).build();
		return doCommit(this::completes);
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

					// Implementation 1:  optimized wrapping + late release (fails maybe because we join all the buffer with DataBufferUtils.join before calling callback.succeeded())
					Flux<DataBuffer> content = Flux.from(contentChunks).map(chunk -> new JettyClientDataBuffer(this.bufferFactory, chunk));

					// Implementation 2: buffer copy (works)
					//Flux<DataBuffer> content = Flux.from(contentChunks).map(chunk -> {
					//	DataBuffer buffer = this.bufferFactory.allocateBuffer(chunk.buffer.capacity());
					//	buffer.write(chunk.buffer);
					//	chunk.callback.succeeded();
					//	return buffer;
					//});

					return Mono.just(new JettyClientHttpResponse(reactiveResponse, content));
			}));
		return Mono.empty();
	}

	public Mono<ClientHttpResponse> getResponse() {
		return this.response;
	}


	public static class JettyClientDataBuffer implements PooledDataBuffer {

		private final AtomicInteger counter = new AtomicInteger(1);

		private final DefaultDataBuffer buffer;

		private final Callback callback;

		private final DefaultDataBufferFactory dataBufferFactory;


		JettyClientDataBuffer(DefaultDataBufferFactory dataBufferFactory, ContentChunk chunk) {
			Assert.notNull(dataBufferFactory, "'dataBufferFactory' must not be null");
			Assert.notNull(chunk, "'chunk' must not be null");
			this.dataBufferFactory = dataBufferFactory;
			this.callback = chunk.callback;
			this.buffer = dataBufferFactory.wrap(chunk.buffer);
		}

		@Override
		public DataBufferFactory factory() {
			return this.dataBufferFactory;
		}

		@Override
		public int indexOf(IntPredicate predicate, int fromIndex) {
			return this.buffer.indexOf(predicate, fromIndex);
		}

		@Override
		public int lastIndexOf(IntPredicate predicate, int fromIndex) {
			return this.buffer.lastIndexOf(predicate, fromIndex);
		}

		@Override
		public int readableByteCount() {
			return this.buffer.readableByteCount();
		}

		@Override
		public int writableByteCount() {
			return this.buffer.writableByteCount();
		}

		@Override
		public int capacity() {
			return this.buffer.capacity();
		}

		@Override
		public DataBuffer capacity(int capacity) {
			return this.buffer.capacity(capacity);
		}

		@Override
		public int readPosition() {
			return this.buffer.readPosition();
		}

		@Override
		public DataBuffer readPosition(int readPosition) {
			return this.buffer.readPosition(readPosition);
		}

		@Override
		public int writePosition() {
			return this.buffer.writePosition();
		}

		@Override
		public DataBuffer writePosition(int writePosition) {
			return this.buffer.writePosition(writePosition);
		}

		@Override
		public byte getByte(int index) {
			return this.buffer.getByte(index);
		}

		@Override
		public byte read() {
			return this.buffer.read();
		}

		@Override
		public DataBuffer read(byte[] destination) {
			return this.buffer.read(destination);
		}

		@Override
		public DataBuffer read(byte[] destination, int offset, int length) {
			return this.buffer.read(destination, offset, length);
		}

		@Override
		public DataBuffer write(byte b) {
			return this.buffer.write(b);
		}

		@Override
		public DataBuffer write(byte[] source) {
			return this.buffer.write(source);
		}

		@Override
		public DataBuffer write(byte[] source, int offset, int length) {
			return this.buffer.write(source, offset, length);
		}

		@Override
		public DataBuffer write(DataBuffer... buffers) {
			return this.buffer.write(buffers);
		}

		@Override
		public DataBuffer write(ByteBuffer... buffers) {
			return this.buffer.write(buffers);
		}

		@Override
		public DataBuffer slice(int index, int length) {
			return this.buffer.slice(index, length);
		}

		@Override
		public ByteBuffer asByteBuffer() {
			return this.buffer.asByteBuffer();
		}

		@Override
		public ByteBuffer asByteBuffer(int index, int length) {
			return this.buffer.asByteBuffer(index, length);
		}

		@Override
		public InputStream asInputStream() {
			return this.buffer.asInputStream();
		}

		@Override
		public InputStream asInputStream(boolean releaseOnClose) {
			return this.buffer.asInputStream(releaseOnClose);
		}

		@Override
		public OutputStream asOutputStream() {
			return this.buffer.asOutputStream();
		}

		@Override
		public PooledDataBuffer retain() {
			this.counter.incrementAndGet();
			return this;
		}

		@Override
		public boolean release() {
			if (this.counter.decrementAndGet() == 0) {
				this.callback.succeeded();
				return true;
			}
			return false;
		}
	}

}
