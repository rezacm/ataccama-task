package com.ataccama.homework.model;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.*;

@Entity
@Setter
@Getter
public class Database {

    @Id
    @GeneratedValue
    private Long id;

    @Column(unique = true, nullable = false)
    private String instanceName;

    @Column(nullable = false)
    private String hostname;

    @Column(nullable = false)
    private Integer port;

    @Column(nullable = false)
    private String databaseName;

    private String username;

    private String password;

}
