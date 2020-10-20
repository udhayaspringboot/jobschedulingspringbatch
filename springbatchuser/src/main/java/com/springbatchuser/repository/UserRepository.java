package com.springbatchuser.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.springbatchuser.model.User;

public interface UserRepository extends JpaRepository<User, Integer> {
}
