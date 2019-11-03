package com.ataccama.homework.repository;

import com.ataccama.homework.model.Database;
import org.springframework.data.jpa.repository.JpaRepository;

public interface DatabaseRepository extends JpaRepository<Database, Long> {

    /**
     * Method that finds database instance by the instanceNameValue
     *
     * @param instanceName name of the DB instance
     * @return Database object
     */
    public Database findByInstanceName(String instanceName);

}
