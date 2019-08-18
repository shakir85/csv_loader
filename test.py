# Create class
class Apple:
    def apple_type(self):
        print("Apple type is " + self.type)


# Object instantiation
# This object is a delegate of that class
# Similar to Java: Apple obj1 = new Apple()
obj1 = Apple()

# Set attributes
obj1.type = "Macintosh"

# Call method from the class Apple
obj1.apple_type()
