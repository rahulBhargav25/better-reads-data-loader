package com.example.betterreadsdataloader.author;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;


@Repository
public interface AuthorRepository extends CassandraRepository<Author, String> {

}
