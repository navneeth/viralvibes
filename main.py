from fasthtml.common import *
from dataclasses import dataclass

app, rt = fast_app()

@dataclass
class User:
    username: str
    email: str
    password: str
def validate_user(user: User):
    errors = []
    if len(user.username) < 3:
        errors.append("Username must be at least 3 characters long")
        if '@' not in user.email:
            errors.append("Invalid email address")
            if len(user.password) < 8:
                errors.append("Password must be at least 8 characters long")
                return errors
@rt("/")
def get():
    return Titled("User Registration",
                  Form(Input(type="text", name="username", placeholder="Username"),
                       Input(type="email", name="email", placeholder="Email"),
                       Input(type="password", name="password", placeholder="Password"),
                       Button("Register", type="submit"),
                       hx_post="/register",
                       hx_target="#result"
                       ),
                       Div(id="result")
                       )
@rt("/register")
def post(user: User):
    errors = validate_user(user)
    if errors:
        return Div(Ul(*[Li(error) for error in errors]), id="result", style="color: red;")
    return Div(f"Registered: {user.username} ({user.email})", id="result", style="color: green;")

serve()