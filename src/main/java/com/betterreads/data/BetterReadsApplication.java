package com.betterreads.data;

import com.betterreads.data.author.Author;
import com.betterreads.data.author.AuthorRepository;
import com.betterreads.data.book.Book;
import com.betterreads.data.book.BookRepository;
import com.betterreads.data.connection.DataStaxAstraProperties;
import org.json.JSONArray;
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
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
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

	@Autowired
	BookRepository bookRepository;

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
		//fetch all author data (I'd prefer to do this in memory)
		//loop through all the works creating a works object
		//update the author id with author name
		//persist results

		Path path = Paths.get(worksDumpLocation);
		List<Book> booksList = new ArrayList<>();
		DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");

		try (Stream<String> stream = Files.lines(path)) {
			stream
				.forEach(line -> {
					String jsonString = line.substring(line.indexOf("{"));
					try {
						JSONObject jsonObject = new JSONObject(jsonString);
						Book book = new Book();

						book.setId(jsonObject.optString("key").replace("/works/",""));

						book.setName(jsonObject.optString("title"));
						JSONObject descriptionObject = jsonObject.optJSONObject("description");
						if (descriptionObject != null) {
							book.setDescription(descriptionObject.optString("value"));
						}
						JSONObject publishedObject = jsonObject.getJSONObject("created");
						if (publishedObject != null) {
							String dateString = publishedObject.getString("value");
							book.setPublishedDate(LocalDate.parse(dateString, dateTimeFormatter));
						}
						JSONArray coversJsonArray = jsonObject.optJSONArray("covers");
						if (coversJsonArray != null) {
							List<String> coverIds = new ArrayList<>();
							for (int i = 0; i < coversJsonArray.length(); i++) {
								coverIds.add(coversJsonArray.getString(i));
							}
							book.setCoverIds(coverIds);
						}

						JSONArray authorsJsonArray = jsonObject.optJSONArray("authors");
						if (authorsJsonArray != null) {
							List<String> authorIds = new ArrayList<>();
							for (int i = 0; i < authorsJsonArray.length(); i++) {
								String authorId = authorsJsonArray.getJSONObject(i)
										.getJSONObject("author")
										.getString("key")
										.replace("/authors/", "");
								authorIds.add(authorId);
							}
							book.setAuthorIds(authorIds);

							List<String> authorNames = authorIds
									.stream()
									.map(id -> authorRepository.findById(id))
									.map(optionalAuthor -> {
										if (optionalAuthor.isEmpty()) {
											return "Unknown Author";
										}
										return optionalAuthor.get().getName();
									}).collect(Collectors.toList());

							book.setAuthorNames(authorNames);
						}
						booksList.add(book);

					} catch (JSONException e) {
						System.out.println("ignoring one json exception");
					}
				});
		} catch (IOException ioe) {
			System.out.println("ignoring one row - moving on");
		}

		bookRepository.saveAll(booksList);
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
		String rp = astraProperties.getRaftaProps();
		System.out.println(rp);
		File raftaFile = astraProperties.getRafaFile();
		Path raftaPath = astraProperties.getRafaFile().toPath();
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}
}
