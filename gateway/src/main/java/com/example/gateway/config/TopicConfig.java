package com.example.gateway.config;

import lombok.Getter;
import lombok.Setter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.*;
import org.springframework.kafka.config.TopicBuilder;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Configuration
@EnableConfigurationProperties({AuthorizeInProps.class, PersistInProps.class})
@Slf4j
public class TopicConfig {

    private final AuthorizeInProps authorizeProps;
    private final PersistInProps persistProps;

    public TopicConfig(AuthorizeInProps authorizeProps, PersistInProps persistProps) {
        this.authorizeProps = authorizeProps;
        this.persistProps = persistProps;
    }

    /** Cria se não existir. */
    @Bean
    public NewTopic authorizeInTopic() {
        return TopicBuilder.name(authorizeProps.getName())
                .partitions(authorizeProps.getPartitions())
                .replicas(authorizeProps.getReplicas().shortValue())
                .build();
    }

    /** Cria se não existir. */
    @Bean
    public NewTopic persistInTopic() {
        return TopicBuilder.name(persistProps.getName())
                .partitions(persistProps.getPartitions())
                .replicas(persistProps.getReplicas().shortValue())
                .build();
    }

    /** Aumenta partições do authorize-in, se precisar. (Diminuir não é possível.) */
    @Bean
    @DependsOn("authorizeInTopic")
    public InitializingBean maybeIncreaseAuthorizePartitions(AdminClient adminClient) {
        return () -> increaseIfNeeded(adminClient, authorizeProps.getName(), authorizeProps.getPartitions());
    }

    /** (Opcional) Aumenta partições do persist-in também. */
    @Bean
    @DependsOn("persistInTopic")
    public InitializingBean maybeIncreasePersistPartitions(AdminClient adminClient) {
        return () -> increaseIfNeeded(adminClient, persistProps.getName(), persistProps.getPartitions());
    }

    private void increaseIfNeeded(AdminClient adminClient, String topic, int desired) throws Exception {
        try {
            var dtr = adminClient.describeTopics(List.of(topic));
            var desc = dtr.values().get(topic).get();
            int current = desc.partitions().size();
            if (desired > current) {
                log.info("Aumentando partições de {}: {} -> {}", topic, current, desired);
                var req = Map.of(topic, NewPartitions.increaseTo(desired));
                var res = adminClient.createPartitions(req);
                KafkaFuture<Void> f = res.values().get(topic);
                f.get();
                log.info("Partições de {} agora: {}", topic, desired);
            } else {
                log.info("Tópico {} já possui {} partições (>= {}).", topic, current, desired);
            }
        } catch (ExecutionException ee) {
            // Se ainda não existir, o NewTopic acima cria; ok ignorar aqui.
            log.warn("Describe falhou para {} (provável criação nova): {}", topic, ee.getMessage());
        }
    }

    /** Usa as props do Spring Kafka Admin já configuradas. */
    @Bean
    @Primary
    public AdminClient adminClient(org.springframework.kafka.core.KafkaAdmin springAdmin) {
        return AdminClient.create(springAdmin.getConfigurationProperties());
    }
}

@ConfigurationProperties(prefix = "app.kafka.topics.authorize-in")
@Getter @Setter
class AuthorizeInProps {
    private String name;
    private Integer partitions;
    private Integer replicas;
}

@ConfigurationProperties(prefix = "app.kafka.topics.persist-in")
@Getter @Setter
class PersistInProps {
    private String name;
    private Integer partitions;
    private Integer replicas;
}
