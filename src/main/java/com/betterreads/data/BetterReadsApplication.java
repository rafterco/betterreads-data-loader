package com.betterreads.data;

import com.betterreads.data.author.Author;
import com.betterreads.data.author.AuthorRepository;
import com.betterreads.data.connection.DataStaxAstraProperties;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;


/**
 * Main application class with main method that runs the Spring Boot app
 */

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterReadsApplication {

	@Value("${datadump.location.author}")
	private String authorDumpLocation;

	@Value("${datadump.location.works}")
	private String worksDumpLocation;

	@Autowired @Lazy
	AuthorRepository authorRepository;

	public static void main(String[] args) {
		SpringApplication.run(BetterReadsApplication.class, args);
	}

	private void initAuthors() {
		Path path = Paths.get(authorDumpLocation);
		List<Author> authorList = new ArrayList<>();

		try (Stream<String> stream = Files.lines(path)) {
			stream
				.forEach(line -> {
					String jsonString = line.substring(line.indexOf("{"));
					try {
						JSONObject jsonObject = new JSONObject(jsonString);
						Author author = new Author();
						author.setName(jsonObject.optString("name"));
						author.setPersonalName(jsonObject.optString("personal_name"));
						author.setId(jsonObject.optString("key").replace("/authors/", ""));

						authorList.add(author);
					} catch (JSONException e) {
						throw new RuntimeException(e);
					}
			});
			authorRepository.saveAll(authorList);

			System.out.println("complete");
		} catch (IOException ioe) {
			throw new IllegalStateException("Unable to read file '" + path + "'", ioe);
		}
	}

	private void initWorks() {

	}

	@PostConstruct
	public void start() {
		initAuthors();
		initWorks();
	}

	/**
	 * This is necessary to have the Spring Boot app use the Astra secure bundle
	 * to connect to the database
	 */
	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}
}
