/*
 * Copyright 2016 Bill Bejeck
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bbejeck.chapter_3;

import bbejeck.chapter_3.service.SecurityDBService;
import bbejeck.clients.producer.MockDataProducer;
import bbejeck.model.Purchase;
import bbejeck.model.PurchasePattern;
import bbejeck.model.RewardAccumulator;
import bbejeck.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

@SuppressWarnings("unchecked")
public class ZMartKafkaStreamsAdvancedReqsApp {

    private static final Logger LOG = LoggerFactory.getLogger(ZMartKafkaStreamsAdvancedReqsApp.class);

    public static void main(String[] args) throws Exception {

        StreamsConfig streamsConfig = new StreamsConfig(getProperties());

        Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
        Serde<PurchasePattern> purchasePatternSerde = StreamsSerdes.PurchasePatternSerde();
        Serde<RewardAccumulator> rewardAccumulatorSerde = StreamsSerdes.RewardAccumulatorSerde();
        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();


        // (previous requirements) purchaseKStream
        KStream<String, Purchase> purchaseKStream = builder.stream( "transactions", Consumed.with(stringSerde, purchaseSerde))
                .mapValues(p -> Purchase.builder(p).maskCreditCard().build());


        // 1 patternKStream
        KStream<String, PurchasePattern> patternKStream = purchaseKStream
                .mapValues(purchase -> PurchasePattern.builder(purchase).build());

        patternKStream.print( Printed.<String, PurchasePattern>toSysOut().withLabel("patterns"));
        patternKStream.to("patterns", Produced.with(stringSerde,purchasePatternSerde));

        // 2 rewardsKStream
        KStream<String, RewardAccumulator> rewardsKStream = purchaseKStream
                .mapValues(purchase -> RewardAccumulator.builder(purchase).build());

        rewardsKStream.print(Printed.<String, RewardAccumulator>toSysOut().withLabel("rewards"));
        rewardsKStream.to("rewards", Produced.with(stringSerde, rewardAccumulatorSerde));



        // Selecting a key for storage and filtering out low dollar purchases

        /*
        [purchases]: 1552805104000,
        Purchase{
            firstName='Bethany', lastName='Nader', customerId='685-26-1546', creditCardNumber='xxxx-xxxx-xxxx-6174',
            itemPurchased='Incredible Rubber Clock', department='Sports & Tools', employeeId='71399', quantity=3, price=111.21,
            purchaseDate=Sun Mar 17 17:45:04 AEDT 2019, zipCode='113469', storeId='597801'
        }
         */
        //TODO: why String???
        //            <K, V, VR(mapped value type)>
        KeyValueMapper<String, Purchase, Long> purchaseDateAsKey = (key, purchase) -> purchase.getPurchaseDate().getTime();

        // modify or create a new key
        KStream<Long, Purchase> filteredKStream = purchaseKStream.filter((key, purchase) -> purchase.getPrice() > 5.00)
                                                                 .selectKey(purchaseDateAsKey);
        //todo: print KeyValueMapper instance in format <Long, Purchase> as <K,V>
        filteredKStream.print(Printed.<Long, Purchase>toSysOut()
                                     .withLabel("purchases"));

        filteredKStream.to("purchases", Produced.with(Serdes.Long(), purchaseSerde));


        // Branching stream for separating out purchases in new departments to their own topics
        Predicate<String, Purchase> isCoffee = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("coffee");
        Predicate<String, Purchase> isElectronics = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("electronics");

        int coffee = 0;
        int electronics = 1;

        // .branch
        KStream<String, Purchase>[] kstreamByDept = purchaseKStream.branch(isCoffee, isElectronics);

        kstreamByDept[coffee].to( "coffee", Produced.with(stringSerde, purchaseSerde));
        kstreamByDept[coffee].print(Printed.<String, Purchase>toSysOut().withLabel( "coffee"));

        kstreamByDept[electronics].to("electronics", Produced.with(stringSerde, purchaseSerde));
        kstreamByDept[electronics].print(Printed.<String, Purchase>toSysOut().withLabel("electronics"));


        // security Requirements to record transactions for certain employee
        // ForeachAction type
        ForeachAction<String, Purchase> purchaseForeachAction = (key, purchase) ->
                SecurityDBService.saveRecord(
                        purchase.getPurchaseDate(),
                        purchase.getEmployeeId(),
                        purchase.getItemPurchased()
                );

        // filter out 0s from purchaseKStream
        purchaseKStream.filter((key, purchase) -> purchase.getEmployeeId().equals("000000"))
                                                          .foreach(purchaseForeachAction);   // foreach-action


        // TODO: used only to produce data for this application, not typical usage
        MockDataProducer.producePurchaseData();
        
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
        LOG.info("ZMart Advanced Requirements Kafka Streams Application Started");
        kafkaStreams.start();
        Thread.sleep(65000);
        LOG.info("Shutting down the Kafka Streams Application now");
        kafkaStreams.close();
        MockDataProducer.shutdown();
    }




    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "Example-Kafka-Streams-Job");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "streams-purchases");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testing-streams-api");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }

}
