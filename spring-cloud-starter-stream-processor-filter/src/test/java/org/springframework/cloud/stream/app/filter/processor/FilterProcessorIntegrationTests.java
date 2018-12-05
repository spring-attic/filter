/*
 * Copyright 2015-2018 the original author or authors.
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

package org.springframework.cloud.stream.app.filter.processor;

import java.util.HashMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.annotation.Bindings;
import org.springframework.cloud.stream.converter.TupleJsonMessageConverter;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.tuple.Tuple;
import org.springframework.tuple.TupleBuilder;
import org.springframework.util.MimeTypeUtils;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.springframework.cloud.stream.test.matcher.MessageQueueMatcher.receivesPayloadThat;

/**
 * Integration tests for Filter Processor.
 *
 * @author Eric Bottard
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Christian Tzolov
 */
@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext
@SpringBootTest
public abstract class FilterProcessorIntegrationTests {

	@Autowired
	@Bindings(FilterProcessorConfiguration.class)
	protected Processor channels;

	@Autowired
	protected MessageCollector collector;

	/**
	 * Validates that the module loads with default properties.
	 */
	public static class UsingNothingIntegrationTests extends FilterProcessorIntegrationTests {

		@Test
		public void test() {
			channels.input().send(new GenericMessage<Object>("hello"));
			channels.input().send(new GenericMessage<Object>("hello world"));
			channels.input().send(new GenericMessage<Object>("hi!"));
			assertThat(collector.forChannel(channels.output()), receivesPayloadThat(is("hello")));
			assertThat(collector.forChannel(channels.output()), receivesPayloadThat(is("hello world")));
			assertThat(collector.forChannel(channels.output()), receivesPayloadThat(is("hi!")));
		}
	}

	@SpringBootTest("filter.expression=payload.length()>5")
	public static class UsingExpressionIntegrationTests extends FilterProcessorIntegrationTests {

		@Test
		public void test() throws InterruptedException {
			channels.input().send(new GenericMessage<Object>("hello"));
			channels.input().send(new GenericMessage<Object>("hello world"));
			channels.input().send(new GenericMessage<Object>("hi!"));
			assertThat(collector.forChannel(channels.output()), receivesPayloadThat(is("hello world")));
			assertThat(collector.forChannel(channels.output()).poll(10, MILLISECONDS), is(nullValue(Message.class)));
		}

		@Test
		public void testTextContentTypeWithOctetPayload() throws InterruptedException {
			Message<byte[]> message1 = MessageBuilder.withPayload("hello".getBytes())
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN).build();
			Message<byte[]> message2 = MessageBuilder.withPayload("hello world".getBytes())
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN).build();
			Message<byte[]> message3 = MessageBuilder.withPayload("hi!".getBytes())
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN).build();

			channels.input().send(message1);
			channels.input().send(message2);
			channels.input().send(message3);
			assertThat(collector.forChannel(channels.output()), receivesPayloadThat(is("hello world")));
			assertThat(collector.forChannel(channels.output()).poll(10, MILLISECONDS), is(nullValue(Message.class)));
		}
	}

	@SpringBootTest("filter.expression=#jsonPath(payload,'$.foo')=='bar'")
	public static class OctetPayloadForJsonTupleContentTypesTests extends FilterProcessorIntegrationTests {

		@Test
		public void testJsonFilterMatch() {
			byte[] payloadMatch = "{\"foo\":\"bar\"}".getBytes();
			Message<byte[]> message = MessageBuilder.withPayload(payloadMatch)
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON).build();
			channels.input().send(message);
			assertThat(collector.forChannel(channels.output()), receivesPayloadThat(is(new String(payloadMatch))));
		}

		@Test
		public void testJsonFilterNotMatch() throws InterruptedException {
			byte[] payloadNotMatch = "{\"foo\":\"NotBar\"}".getBytes();
			Message<byte[]> messageNotMatch = MessageBuilder.withPayload(payloadNotMatch)
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON).build();
			channels.input().send(messageNotMatch);
			assertThat(collector.forChannel(channels.output()).poll(10, MILLISECONDS), is(nullValue(Message.class)));
		}

		@Test
		public void testTupleMatch() {
			Tuple tuple = TupleBuilder.tuple().of("foo", "bar");
			Message<byte[]> message = (Message<byte[]>) new TupleJsonMessageConverter(
					new ObjectMapper()).toMessage(tuple, new MessageHeaders(new HashMap<>()));
			channels.input().send(message);
			assertThat(collector.forChannel(channels.output()), receivesPayloadThat(is("{\"foo\":\"bar\"}".getBytes())));
		}

		@Test
		public void testTupleNotMatch() throws InterruptedException {
			Tuple tuple = TupleBuilder.tuple().of("foo", "NotBar");
			Message<byte[]> message = (Message<byte[]>) new TupleJsonMessageConverter(
					new ObjectMapper()).toMessage(tuple, new MessageHeaders(new HashMap<>()));
			channels.input().send(message);
			assertThat(collector.forChannel(channels.output()).poll(10, MILLISECONDS), is(nullValue(Message.class)));
		}
	}

	@SpringBootApplication
	public static class FilterProcessorApplication {

	}

}
