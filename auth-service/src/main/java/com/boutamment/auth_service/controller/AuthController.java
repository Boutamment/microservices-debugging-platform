package com.boutamment.auth_service.controller;

import com.boutamment.auth_service.model.Role;
import com.boutamment.auth_service.model.User;
import com.boutamment.auth_service.service.JwtService;
import com.boutamment.auth_service.service.UserService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/auth")
@CrossOrigin(origins = "*")
@RequiredArgsConstructor
public class AuthController {

    private final AuthenticationManager authManager;
    private final UserDetailsService userDetailsService;
    private final JwtService jwtService;
    private final PasswordEncoder passwordEncoder;
    private final UserService userService;

    @PostMapping("/login")
    public ResponseEntity<?> login(@Valid @RequestBody LoginRequest req) {
        authManager.authenticate(
            new UsernamePasswordAuthenticationToken(req.email(), req.password())
        );
        User user = (User) userDetailsService.loadUserByUsername(req.email());
        String token = jwtService.generateToken(user);
        log.info("User logged in: {}", user.getEmail());
        return ResponseEntity.ok(Map.of(
            "token", token,
            "userId", user.getId(),
            "role", user.getRole()
        ));
    }

    @PostMapping("/register")
    public ResponseEntity<?> register(@Valid @RequestBody RegisterRequest req) {
        User user = userService.register(req.email(), req.password(), req.role());
        String token = jwtService.generateToken(user);
        log.info("New user registered: {}", user.getEmail());
        return ResponseEntity.ok(Map.of(
            "token", token,
            "userId", user.getId()
        ));
    }

    @GetMapping("/validate")
    public ResponseEntity<?> validate(@RequestHeader("Authorization") String authHeader) {
        String token = authHeader.substring(7);
        boolean valid = jwtService.isTokenValid(token);
        return ResponseEntity.ok(Map.of("valid", valid));
    }

    public record LoginRequest(
        @NotBlank @Email String email,
        @NotBlank String password
    ) {}

    public record RegisterRequest(
        @NotBlank @Email String email,
        @NotBlank @Size(min = 8) String password,
        Role role
    ) {}
}
