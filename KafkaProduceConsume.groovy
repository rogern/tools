
import groovy.beans.Bindable
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.swing.SwingBuilder
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed

import javax.swing.JFrame
import java.awt.BorderLayout

@Grapes(
    @Grab(group = 'org.apache.kafka', module = 'kafka-streams', version = '2.4.1', classifier = 'javadoc')
)

class IsJsonCategory {
  static def isJson(String del) {
    def normalize = { it.replaceAll("\\s", "") }
    try {
      normalize(del) == normalize(JsonOutput.toJson(new JsonSlurper().parseText(del)))
    } catch (e) {
      false
    }
  }
}

String.metaClass.mixin(IsJsonCategory)

class KafkaProduceConsumer {
  static props = [(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG):"localhost:9092"]
  static serdes = Serdes.serdeFrom(new StringSerializer(), new StringDeserializer())

  static streamFromTopic(String topic, Model appender) {
    def builder = new StreamsBuilder()
    def stream = builder.stream(topic, Consumed.with(serdes, serdes))
    stream.foreach { a, b ->
      println(a + " - " + b)
      if (b.isJson()) appender.out = "${appender.out}${JsonOutput.prettyPrint(b)}\n"
      else appender.out = "${appender.out}$b\n"
    }

    def p = new StreamsConfig(props + [(StreamsConfig.APPLICATION_ID_CONFIG):"consumer-producer-" + topic])
    def streams = new KafkaStreams(builder.build(), p)
    streams.start()
  }

  static send(String topic, String value) {
    def producer = new KafkaProducer(props, serdes.serializer(), serdes.serializer())
    producer.send(new ProducerRecord(topic, value), {
      println("Callback: $it")
    })
    producer.close()
  }
}

@Bindable
class Model {
  String topic = "consumetopic"
  String out
  String produceTopic = "producetopic"
  String producePayload
}

def openWindow() {
  def swing = new SwingBuilder()
  def model = new Model()

  def kafka = new KafkaProduceConsumer()

  swing.edt {
    lookAndFeel 'system'
    frame(title: 'Kafka Producer/Consumer', defaultCloseOperation: JFrame.EXIT_ON_CLOSE, show: true, size: [1000, 500], alwaysOnTop: true) {
      boxLayout()
      panel() {
        flowLayout()
        panel(constraints: BorderLayout.WEST, border: compoundBorder([emptyBorder(10), titledBorder('Produce to:')])) {
          tableLayout {
            tr {
              td {
                label 'Topic'
              }
              td {
                textField model.produceTopic, id: 'produceTopicField', columns: 24
              }
              td {
                button text: 'Send', actionPerformed: {
                  kafka.send(model.produceTopic, model.producePayload)
                }
              }
            }
          }
        }
        panel(constraints: BorderLayout.SOUTH, border: compoundBorder([emptyBorder(10), titledBorder('Payload:')])) {
          tableLayout {
            tr {
              td {
                scrollPane {
                  textArea model.producePayload, id: 'producePayloadArea', columns: 34, rows: 20, editable: true
                }
              }
            }
          }
        }
      }
      panel() {
        flowLayout()
        panel(constraints: BorderLayout.EAST, border: compoundBorder([emptyBorder(10), titledBorder('Consume from:')])) {
          tableLayout {
            tr {
              td {
                label 'Topic'
              }
              td {
                textField model.topic, id: 'topicField', columns: 24
              }
              td {
                button text: 'Start', actionPerformed: {
                  println("Set topic: ${model.topic}")
                  kafka.streamFromTopic(model.topic, model)
                }
              }
            }
          }
        }
        panel(constraints: BorderLayout.SOUTH, border: compoundBorder([emptyBorder(10), titledBorder('Messages:')])) {
          tableLayout {
            tr {
              td {
                scrollPane {
                  textArea text: bind(source: model, sourceProperty: 'out'), id: 'outArea', columns: 34, rows: 20, editable: false
                }
              }
            }
          }
        }
      }
    }

    bean model,
        'topic': bind { topicField.text },
        'produceTopic': bind { produceTopicField.text },
        'producePayload': bind { producePayloadArea.text }
  }
}

openWindow()

