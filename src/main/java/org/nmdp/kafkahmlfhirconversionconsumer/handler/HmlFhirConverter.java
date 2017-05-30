package org.nmdp.kafkahmlfhirconversionconsumer.handler;

/**
 * Created by Andrew S. Brown, Ph.D., <andrew@nmdp.org>, on 5/30/17.
 * <p>
 * process-kafka-hml-fhir-conversion-consumer
 * Copyright (c) 2012-2017 National Marrow Donor Program (NMDP)
 * <p>
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation; either version 3 of the License, or (at
 * your option) any later version.
 * <p>
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; with out even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 * <p>
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library;  if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA.
 * <p>
 * > http://www.fsf.org/licensing/licenses/lgpl.html
 * > http://www.opensource.org/licenses/lgpl-license.php
 */

import javax.inject.Singleton;

import java.io.Closeable;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.nmdp.hmlfhirconvertermodels.dto.Hml;
import org.nmdp.servicekafkaproducermodel.models.KafkaMessage;
import org.nmdp.servicekafkaproducermodel.models.KafkaMessagePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.nmdp.kafkaconsumer.handler.KafkaMessageHandler;

@Singleton
public class HmlFhirConverter implements KafkaMessageHandler, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(HmlFhirConverter.class);
    private final ConcurrentMap<String, LinkedBlockingQueue<WorkItem>> workQueueMap = new ConcurrentHashMap<>();
    private static final ThreadLocal<DecimalFormat> DF = ThreadLocal.withInitial(() -> new DecimalFormat("####################"));
    private static final ThreadLocal<ObjectMapper> OBJECT_MAPPER = ThreadLocal.withInitial(ObjectMapper::new);

    public HmlFhirConverter() throws IOException {

    }

    private String getSenderKey(String topic, int partition) {
        return topic + "-" + DF.get().format(partition);
    }

    private LinkedBlockingQueue<WorkItem> getWorkQueue(String senderKey) {
        return workQueueMap.computeIfAbsent(senderKey, k -> new LinkedBlockingQueue<>());
    }

    @Override
    public void process(String topic, int partition, long offset, byte[] key, byte[] payload) throws Exception {
        String senderKey = getSenderKey(topic, partition);
        LinkedBlockingQueue<WorkItem> queue = getWorkQueue(senderKey);
        KafkaMessage message;

        try {
            message = OBJECT_MAPPER.get().reader(KafkaMessage.class).readValue(payload);
        } catch (Exception e) {
            LOG.error("Error parsing message " + topic + "-" + DF.get().format(partition) + ":" + DF.get().format(offset), e);
            return;
        }

        try {
            KafkaMessagePayload messagePayload = message.getPayload();
            queue.put(new WorkItem((Hml) messagePayload.getModel(), messagePayload.getModelId()));
        } catch (InterruptedException ex) {
            LOG.error("Error committing to queue, interrupted: ", ex);
        }
    }

    public void commit(String topic, int partition, long offset) throws Exception {
        String senderKey = getSenderKey(topic, partition);
        LinkedBlockingQueue<WorkItem> queue = getWorkQueue(senderKey);
        List<WorkItem> work = new ArrayList<>(queue.size());
        queue.drainTo(work);

        try {
            commitWork(work);
        } catch (Exception ex) {
            LOG.error("Error committing work: ", ex);
        }
    }

    private void commitWork(List<WorkItem> work) throws IOException {
        try {
            work.stream()
                .filter(Objects::nonNull)
                .forEach(item -> convertHmlToFhir(item));
        } catch (Exception ex) {
            LOG.error("Error processing table: ", ex);
        }
    }

    private void convertHmlToFhir(WorkItem item) {
        Hml hml = item.getHml();

        if (hml == null) {
            hml = getHmlFromMongo(item.getHmlId());
        }

        // TODO: finish call to convert
    }

    private Hml getHmlFromMongo(String hmlId) {
        return new org.nmdp.hmlfhirconvertermodels.domain.Hml().toDto(new org.nmdp.hmlfhirconvertermodels.domain.Hml());

        // TODO: implement call to database
    }

    @Override
    public void rollback(String topic, int partition, long offset) throws Exception {
        String senderKey = getSenderKey(topic, partition);
        LinkedBlockingQueue<WorkItem> queue = getWorkQueue(senderKey);
        queue.clear();
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {

        }
    }

    @Override
    public String toString() {
        return "HmlFhirConverter[]";
    }

    private static class WorkItem {
        private final Hml hml;
        private final String hmlId;

        public WorkItem(Hml hml, String hmlId) {
            this.hml = hml;
            this.hmlId = hmlId;
        }

        public Hml getHml() {
            return hml;
        }

        public String getHmlId() {
            return hmlId;
        }
    }
}
