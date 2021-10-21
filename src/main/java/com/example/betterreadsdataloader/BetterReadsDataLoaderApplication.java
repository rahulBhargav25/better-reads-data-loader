package com.example.betterreadsdataloader;

import com.example.betterreadsdataloader.author.Author;
import com.example.betterreadsdataloader.author.AuthorRepository;
import com.example.betterreadsdataloader.book.Book;
import com.example.betterreadsdataloader.book.BookRepository;
import com.example.betterreadsdataloader.connection.DataStaxAstraProperties;
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

import javax.annotation.PostConstruct;
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


@SpringBootApplication(scanBasePackages = {"com.example.betterreadsdataloader.author",
        "com.example.betterreadsdataloader.connection",
        "com.example.betterreadsdataloader.book"})
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterReadsDataLoaderApplication {


    @Autowired
    AuthorRepository authorRepository;

    @Autowired
    BookRepository bookRepository;

    @Value("${datadump.location.author}")
    private String authorDumpLocation;

    @Value("${datadump.location.works}")
    private String worksDumpLocation;

    public static void main(String[] args) {

        SpringApplication.run(BetterReadsDataLoaderApplication.class, args);
    }

    private void initAuthors() {
        Path path = Paths.get(authorDumpLocation);
        try(Stream<String> Lines = Files.lines(path)) {
            Lines.forEach(Line -> {
                //Read and parse the line
                String jsonString = Line.substring(Line.indexOf("{"));
                JSONObject jsonObject = null;
                try {
                    jsonObject = new JSONObject(jsonString);
                    //construct  Another Object
                    Author author = new Author();
                    author.setName(jsonObject.optString("name"));
                    author.setPersonalName(jsonObject.optString("personal_name"));
                    author.setId(jsonObject.optString("key").replace("/authors/", ""));

                    //Persist Using Repo
                    authorRepository.save(author);
                    System.out.println("ID : "+author.getId()+"authors are being saved " + author.getName());
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            });

        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    private void initWorks() {
        Path path = Paths.get(worksDumpLocation);
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
        try(Stream<String> Lines = Files.lines(path)) {
            Lines.limit(50).forEach(Line -> {
                //Read and parse the line
                String jsonString = Line.substring(Line.indexOf("{"));
                JSONObject jsonObject = null;
                try {
                    jsonObject = new JSONObject(jsonString);
                    //construct  Another Object
                    Book book = new Book();
                    book.setId(jsonObject.getString("key").replace("/works/",""));
                    book.setName(jsonObject.optString("title"));
                    JSONObject descriptionObj =  jsonObject.optJSONObject("description");
                    if(descriptionObj != null) {

                        book.setDescription(descriptionObj.optString("value"));
                    }

                    JSONObject publishedObj = jsonObject.optJSONObject("created");
                    if(publishedObj != null) {
                        String datestr = publishedObj.getString("value");
                        book.setPublishedDate(LocalDate.parse(datestr, dateFormat));
                    }

                   JSONArray coversJsonArray = jsonObject.optJSONArray("covers");
                    if(coversJsonArray != null) {
                        List<String> coverIds = new ArrayList<>();
                        for(int i = 0; i < coversJsonArray.length(); i++) {
                            coverIds.add(coversJsonArray.getString(i));
                        }
                        book.setCoverIds(coverIds);
                    }

                    JSONArray authorsJsonArray = jsonObject.optJSONArray("authors");
                    if(authorsJsonArray != null) {
                        List<String> authorIds = new ArrayList<>();
                        for(int i = 0; i < authorsJsonArray.length(); i++) {
                         String authorId = authorsJsonArray.getJSONObject(i).getJSONObject("author").
                                 getString("key")
                                 .replace("/authors/", "");
                                 authorIds.add(authorId);
                        }
                        book.setAuthorId(authorIds);

                        List<String> authorNames = authorIds.stream().map(id -> authorRepository.findById(id))
                                .map(optionalAuthor -> {
                                    if(!optionalAuthor.isPresent()) return "unknown Author";
                                    return optionalAuthor.get().getName();
                                }).collect(Collectors.toList());
                        book.setAuthorNames(authorNames);
                    }


                    //Persist Using Repo
                    System.out.println(book.getId()+" Books are being saved " + book.getName());
                    bookRepository.save(book);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

        } catch(IOException e) {
            e.printStackTrace();
        }
    }


    @PostConstruct
    public void start() {
        //initAuthors();
        initWorks();
    }





    @Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
        Path bundle = astraProperties.getSecureConnectBundle().toPath();
        return builder -> builder.withCloudSecureConnectBundle(bundle);
    }

}
